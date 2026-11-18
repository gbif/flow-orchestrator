/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.orchestrator.app.completion;

import org.gbif.api.model.pipelines.StepType;
import org.gbif.orchestrator.api.step.Outcome;
import org.gbif.orchestrator.api.step.Report;
import org.gbif.orchestrator.api.step.StepCompleted;
import org.gbif.orchestrator.api.step.StepHeader;
import org.gbif.orchestrator.app.command.DecisionApplier;
import org.gbif.orchestrator.app.command.DecisionNotes;
import org.gbif.orchestrator.app.config.OrchestratorProperties;
import org.gbif.orchestrator.app.config.WorkflowDefinition;
import org.gbif.orchestrator.app.config.WorkflowDefinitions;
import org.gbif.orchestrator.app.interpret.ReportInterpreter;
import org.gbif.orchestrator.app.interpret.ReportInterpreters;
import org.gbif.orchestrator.app.persistence.RetryingTransactions;
import org.gbif.orchestrator.app.persistence.StepAttemptRepository;
import org.gbif.orchestrator.app.persistence.StepExecution;
import org.gbif.orchestrator.app.persistence.WorkflowExecution;
import org.gbif.orchestrator.app.persistence.WorkflowReport;
import org.gbif.orchestrator.app.persistence.WorkflowReportRepository;
import org.gbif.orchestrator.app.persistence.WorkflowStateLoader;
import org.gbif.orchestrator.engine.DagWorkflowEngine;
import org.gbif.orchestrator.engine.WorkflowChange;
import org.gbif.orchestrator.engine.WorkflowDecision;
import org.gbif.orchestrator.engine.WorkflowEvent;
import org.gbif.orchestrator.engine.WorkflowState.Status;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Handles a {@link StepCompleted} in one transaction: records the attempt's end and its reports,
 * lets the engine decide, and applies the decision (next dispatches, terminal status, {@code
 * WorkflowFinished}). Retried from a fresh read on concurrent changes.
 *
 * <p>A successful step's reports go through its {@link ReportInterpreters report interpreter}: a
 * rejection skips everything after the step and ends the workflow {@code FINISHED}, rejected; an
 * unreadable report fails the step.
 */
@Service
public class CompletionService {

  private static final Logger LOG = LoggerFactory.getLogger(CompletionService.class);

  private final RetryingTransactions transactions;
  private final WorkflowStateLoader loader;
  private final StepAttemptRepository attempts;
  private final WorkflowReportRepository reports;
  private final DecisionApplier applier;
  private final WorkflowDefinitions definitions;
  private final OrchestratorProperties properties;
  private final Clock clock;
  private final ReportInterpreters interpreters;

  public CompletionService(
      RetryingTransactions transactions,
      WorkflowStateLoader loader,
      StepAttemptRepository attempts,
      WorkflowReportRepository reports,
      DecisionApplier applier,
      WorkflowDefinitions definitions,
      OrchestratorProperties properties,
      Clock clock,
      ReportInterpreters interpreters) {
    this.interpreters = interpreters;
    this.transactions = transactions;
    this.loader = loader;
    this.attempts = attempts;
    this.reports = reports;
    this.applier = applier;
    this.definitions = definitions;
    this.properties = properties;
    this.clock = clock;
  }

  /**
   * @throws UnknownWorkflowException if the completion cannot belong to this instance
   */
  public CompletionResult handle(StepCompleted completed) {
    StepHeader header = completed.header();
    CompletionResult result =
        transactions.execute(
            "messageId=" + completed.messageId(), () -> handleOnce(completed, header));
    LOG.info(
        "Completion handled messageId={} workflow={} step={} stepAttempt={} outcome={} result={}",
        completed.messageId(),
        header.workflowId(),
        header.stepType(),
        header.stepAttempt(),
        completed.outcome(),
        result);
    return result;
  }

  private CompletionResult handleOnce(StepCompleted completed, StepHeader header) {
    WorkflowStateLoader.Loaded loaded =
        loader
            .find(header.workflowId())
            .orElseThrow(
                () -> new UnknownWorkflowException("Unknown workflow " + header.workflowId()));
    WorkflowExecution execution = loaded.execution();
    checkBelongsHere(execution, header);
    StepExecution step =
        loaded.steps().stream()
            .filter(s -> s.stepType() == header.stepType())
            .findFirst()
            .orElseThrow(
                () ->
                    new UnknownWorkflowException(
                        "Workflow " + execution.id() + " has no step " + header.stepType()));

    if (step.currentAttempt() != header.stepAttempt()
        || (step.status() != Status.QUEUED && step.status() != Status.RUNNING)) {
      LOG.info(
          "Ignoring stale completion messageId={} workflow={} step={} stepAttempt={}: "
              + "current attempt {} is {}",
          completed.messageId(),
          execution.id(),
          header.stepType(),
          header.stepAttempt(),
          step.currentAttempt(),
          step.status());
      return CompletionResult.IGNORED;
    }

    WorkflowDefinition definition = definitionOf(execution);

    // Steps report facts, the orchestrator decides (section 4a). A report that cannot be read
    // fails the step; it is never read as "continue".
    Optional<ReportInterpreter.Rejection> rejection = Optional.empty();
    String diagnostic = completed.diagnostic();
    boolean failed = completed.outcome() != Outcome.SUCCEEDED;
    if (!failed) {
      try {
        rejection = interpreters.interpret(execution.domain(), header.stepType(), completed.reports());
      } catch (ReportInterpreter.UnreadableReportException e) {
        failed = true;
        diagnostic = "Unreadable report: " + e.getMessage();
      }
    }

    attempts.complete(
        execution.id(),
        header.stepType(),
        header.stepAttempt(),
        failed ? Status.FAILED : Status.FINISHED,
        completed.outcome(),
        diagnostic,
        completed.outputs(),
        completed.startedAt(),
        completed.finishedAt());
    // Stored in the same transaction as the decision, so every decision has its report.
    storeReports(completed, execution);

    WorkflowEvent event =
        failed ? WorkflowEvent.failed(header.stepType()) : WorkflowEvent.completed(header.stepType());
    List<WorkflowChange> changes =
        rejection.isPresent() ? skipAfter(definition, loaded, header.stepType()) : List.of();
    WorkflowDecision decision =
        new DagWorkflowEngine(definition.dag()).evaluate(loaded.state(), event, changes);
    DecisionNotes notes =
        rejection.isPresent()
            ? rejectedNotes(header, rejection.get(), decision)
            : failed ? failureNotes(header, diagnostic) : DecisionNotes.none();
    applier.apply(loaded, decision, notes);
    return CompletionResult.PROCESSED;
  }

  /** SKIP_FROM for each pending step that directly depends on {@code step}. */
  private static List<WorkflowChange> skipAfter(
      WorkflowDefinition definition, WorkflowStateLoader.Loaded loaded, StepType step) {
    Set<StepType> pending = EnumSet.noneOf(StepType.class);
    loaded.state().steps().stream()
        .filter(s -> s.status() == Status.PENDING)
        .forEach(s -> pending.add(s.type()));
    List<WorkflowChange> changes = new ArrayList<>();
    for (StepType type : definition.dag().topologicalOrder()) {
      if (pending.contains(type)
          && definition.dag().require(type).dependencies().contains(step)) {
        changes.add(WorkflowChange.skipFrom(type));
      }
    }
    return changes;
  }

  private static DecisionNotes rejectedNotes(
      StepHeader header, ReportInterpreter.Rejection rejection, WorkflowDecision decision) {
    boolean finishes =
        decision.actions().stream()
            .anyMatch(
                action ->
                    action.kind() == WorkflowDecision.Kind.SET_WORKFLOW_STATUS
                        && action.status() == Status.FINISHED);
    if (!finishes) {
      // Only possible with parallel branches still running; the verdict is not kept yet.
      LOG.warn(
          "Rejection by {} does not end workflow {} yet; the verdict is lost: {}",
          header.stepType(),
          header.workflowId(),
          rejection.reason());
    }
    LOG.info(
        "Workflow rejected workflow={} step={} reason={}",
        header.workflowId(),
        header.stepType(),
        rejection.reason());
    return new DecisionNotes(
        true, rejection.reason(), Map.of(), "Not run: " + rejection.reason());
  }

  private void checkBelongsHere(WorkflowExecution execution, StepHeader header) {
    if (execution.domain() != properties.getDomain()) {
      throw new UnknownWorkflowException(
          "Workflow " + execution.id() + " belongs to domain " + execution.domain());
    }
    if (!execution.datasetKey().equals(header.datasetKey())
        || execution.attempt() != header.attempt()) {
      throw new UnknownWorkflowException(
          "Workflow "
              + execution.id()
              + " is for dataset "
              + execution.datasetKey()
              + " attempt "
              + execution.attempt()
              + ", not "
              + header.datasetKey()
              + " attempt "
              + header.attempt());
    }
  }

  /**
   * The definition the execution was started with. A changed definition (other hash) would make
   * the engine decide on a different DAG than the stored steps, so it is refused.
   */
  private WorkflowDefinition definitionOf(WorkflowExecution execution) {
    WorkflowDefinition definition =
        definitions
            .byName(execution.workflowName())
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Workflow definition " + execution.workflowName() + " is not configured"));
    if (!definition.hash().equals(execution.definitionHash())) {
      throw new IllegalStateException(
          "Workflow definition "
              + execution.workflowName()
              + " changed since workflow "
              + execution.id()
              + " started");
    }
    return definition;
  }

  private void storeReports(StepCompleted completed, WorkflowExecution execution) {
    Instant now = clock.instant();
    StepHeader header = completed.header();
    for (Report report : completed.reports()) {
      String comment =
          report.comment() != null
              ? report.comment()
              : header.stepType() + " attempt " + header.stepAttempt();
      reports.upsert(
          new WorkflowReport(
              execution.datasetKey(),
              execution.attempt(),
              report.type(),
              report.content(),
              comment,
              now,
              now));
    }
  }

  private static DecisionNotes failureNotes(StepHeader header, String diagnostic) {
    String failure = header.stepType() + " failed: " + diagnostic;
    return new DecisionNotes(
        false,
        failure,
        Map.of(header.stepType(), diagnostic),
        "Not run: " + header.stepType() + " failed");
  }
}
