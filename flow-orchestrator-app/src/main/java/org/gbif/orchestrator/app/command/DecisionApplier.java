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
package org.gbif.orchestrator.app.command;

import org.gbif.api.model.pipelines.StepType;
import org.gbif.orchestrator.api.event.WorkflowFinished;
import org.gbif.orchestrator.api.event.WorkflowResult;
import org.gbif.orchestrator.api.step.StepHeader;
import org.gbif.orchestrator.app.config.OrchestratorProperties;
import org.gbif.orchestrator.app.json.ContractJson;
import org.gbif.orchestrator.app.persistence.OptimisticConflictException;
import org.gbif.orchestrator.app.persistence.OutboxMessage;
import org.gbif.orchestrator.app.persistence.OutboxRepository;
import org.gbif.orchestrator.app.persistence.OutboxWritten;
import org.gbif.orchestrator.app.persistence.StepAttempt;
import org.gbif.orchestrator.app.persistence.StepAttemptRepository;
import org.gbif.orchestrator.app.persistence.StepExecution;
import org.gbif.orchestrator.app.persistence.StepExecutionRepository;
import org.gbif.orchestrator.app.persistence.WorkflowExecution;
import org.gbif.orchestrator.app.persistence.WorkflowExecutionRepository;
import org.gbif.orchestrator.app.persistence.WorkflowReportRepository;
import org.gbif.orchestrator.app.persistence.WorkflowStateLoader;
import org.gbif.orchestrator.app.request.StepContext;
import org.gbif.orchestrator.app.request.StepRequests;
import org.gbif.orchestrator.engine.WorkflowDecision;
import org.gbif.orchestrator.engine.WorkflowState.Status;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

/**
 * Persists a {@link WorkflowDecision} inside the caller's transaction: step and workflow statuses
 * with their reasons, new step attempts, an outbox row for every dispatch, and a {@link
 * WorkflowFinished} outbox row when the workflow becomes terminal. Ends with the optimistic version
 * check on the workflow execution.
 */
@Component
public class DecisionApplier {

  private static final Logger LOG = LoggerFactory.getLogger(DecisionApplier.class);

  private final WorkflowExecutionRepository executions;
  private final StepExecutionRepository steps;
  private final StepAttemptRepository attempts;
  private final OutboxRepository outbox;
  private final WorkflowReportRepository reports;
  private final StepRequests requests;
  private final OrchestratorProperties properties;
  private final ContractJson json;
  private final ApplicationEventPublisher events;
  private final Clock clock;

  public DecisionApplier(
      WorkflowExecutionRepository executions,
      StepExecutionRepository steps,
      StepAttemptRepository attempts,
      OutboxRepository outbox,
      WorkflowReportRepository reports,
      StepRequests requests,
      OrchestratorProperties properties,
      ContractJson json,
      ApplicationEventPublisher events,
      Clock clock) {
    this.executions = executions;
    this.steps = steps;
    this.attempts = attempts;
    this.outbox = outbox;
    this.reports = reports;
    this.requests = requests;
    this.properties = properties;
    this.json = json;
    this.events = events;
    this.clock = clock;
  }

  /** Applies a decision that carries no reasons or verdict. */
  public void apply(WorkflowStateLoader.Loaded loaded, WorkflowDecision decision) {
    apply(loaded, decision, DecisionNotes.none());
  }

  /**
   * Applies the decision to the loaded workflow.
   *
   * @throws OptimisticConflictException if the workflow changed since it was loaded
   * @throws IllegalStateException if the notes reject a workflow that does not end {@code
   *     FINISHED}
   */
  public void apply(
      WorkflowStateLoader.Loaded loaded, WorkflowDecision decision, DecisionNotes notes) {
    WorkflowExecution execution = loaded.execution();
    UUID id = execution.id();
    Instant now = clock.instant();
    Map<StepType, StepExecution> current = new EnumMap<>(StepType.class);
    loaded.steps().forEach(step -> current.put(step.stepType(), step));
    Status workflowStatus = execution.status();
    boolean wroteOutbox = false;

    for (WorkflowDecision.Action action : decision.actions()) {
      switch (action.kind()) {
        case SET_STEP_STATUS -> {
          StepExecution step = current.get(action.step());
          int attemptNo = step.currentAttempt();
          if (action.status() == Status.QUEUED) {
            attemptNo++;
            attempts.insert(
                new StepAttempt(
                    id, action.step(), attemptNo, Status.QUEUED, null, null, null, now, null,
                    null));
          }
          String reason = stepReason(notes, action.step(), action.status());
          steps.update(id, action.step(), action.status(), attemptNo, now, reason);
          current.put(
              action.step(),
              new StepExecution(
                  id, action.step(), step.stepOrder(), action.status(), attemptNo, now, reason));
          LOG.info(
              "Step status workflow={} step={} from={} to={} attempt={} reason={}",
              id,
              action.step(),
              step.status(),
              action.status(),
              attemptNo,
              reason);
        }
        case SET_WORKFLOW_STATUS -> {
          if (workflowStatus != action.status()) {
            LOG.info("Workflow status workflow={} from={} to={}", id, workflowStatus, action.status());
          }
          workflowStatus = action.status();
        }
        case DISPATCH -> {
          dispatch(execution, current, action.step(), now);
          wroteOutbox = true;
        }
      }
    }

    boolean becameTerminal = isTerminal(workflowStatus) && !isTerminal(execution.status());
    boolean rejected = execution.rejected();
    String reason = execution.reason();
    if (becameTerminal) {
      if (notes.rejected() && workflowStatus != Status.FINISHED) {
        throw new IllegalStateException(
            "A workflow can only be rejected when it finishes, not when it ends " + workflowStatus);
      }
      rejected = notes.rejected();
      reason = notes.workflowReason();
    }
    Instant finished = isTerminal(workflowStatus) ? now : null;
    if (becameTerminal) {
      finished(execution, workflowStatus, rejected, reason, now);
      wroteOutbox = true;
    }

    if (!executions.updateIfVersion(
        id, execution.version(), workflowStatus, now, finished, rejected, reason)) {
      throw new OptimisticConflictException(id, execution.version());
    }
    if (wroteOutbox) {
      events.publishEvent(new OutboxWritten());
    }
  }

  private static String stepReason(DecisionNotes notes, StepType step, Status status) {
    String reason = notes.stepReasons().get(step);
    if (reason == null && (status == Status.SKIPPED || status == Status.ABORTED)) {
      reason = notes.defaultStepReason();
    }
    return reason;
  }

  private void dispatch(
      WorkflowExecution execution, Map<StepType, StepExecution> current, StepType type, Instant now) {
    StepExecution step = current.get(type);
    OrchestratorProperties.Destination destination =
        properties.getSteps().get(step.stepType()).getRequest();
    StepHeader header =
        new StepHeader(
            execution.id(),
            execution.datasetKey(),
            execution.attempt(),
            step.stepType(),
            step.currentAttempt());
    outbox.insert(
        new OutboxMessage(
            null,
            UUID.randomUUID(),
            execution.id(),
            step.stepType(),
            step.currentAttempt(),
            destination.getExchange(),
            destination.getRoutingKey(),
            json.write(requests.create(context(header, execution, current))),
            now,
            null,
            0,
            null,
            null));
    LOG.info(
        "Dispatch queued workflow={} step={} attempt={} exchange={} routingKey={}",
        execution.id(),
        step.stepType(),
        step.currentAttempt(),
        destination.getExchange(),
        destination.getRoutingKey());
  }

  /**
   * The request context: the outputs of each finished step's current attempt, latest step first.
   * Steps finished earlier in this transaction are visible, since attempts are written first.
   */
  private StepContext context(
      StepHeader header, WorkflowExecution execution, Map<StepType, StepExecution> current) {
    Map<StepType, Map<String, String>> outputs = new EnumMap<>(StepType.class);
    for (StepAttempt attempt : attempts.findByExecution(execution.id())) {
      StepExecution step = current.get(attempt.stepType());
      if (step != null && attempt.attemptNo() == step.currentAttempt()) {
        outputs.put(attempt.stepType(), attempt.outputs());
      }
    }
    List<StepContext.FinishedStep> finished = new ArrayList<>();
    current.values().stream()
        .filter(step -> step.status() == Status.FINISHED)
        .sorted(Comparator.comparingInt(StepExecution::stepOrder).reversed())
        .forEach(
            step ->
                finished.add(
                    new StepContext.FinishedStep(
                        step.stepType(), step.stepOrder(), outputs.get(step.stepType()))));
    return new StepContext(header, execution, finished);
  }

  /** Writes the {@link WorkflowFinished} event for a workflow that just became terminal. */
  private void finished(
      WorkflowExecution execution, Status status, boolean rejected, String reason, Instant now) {
    OrchestratorProperties.Destination destination =
        properties.getMessaging().getWorkflowFinished();
    WorkflowFinished event =
        new WorkflowFinished(
            execution.id(),
            execution.domain(),
            execution.datasetKey(),
            execution.attempt(),
            result(status),
            rejected,
            reason,
            reports.findTypes(execution.datasetKey(), execution.attempt()),
            now);
    outbox.insert(
        new OutboxMessage(
            null,
            UUID.randomUUID(),
            execution.id(),
            null,
            null,
            destination.getExchange(),
            destination.getRoutingKey(),
            json.write(event),
            now,
            null,
            0,
            null,
            null));
    LOG.info(
        "Workflow finished workflow={} result={} rejected={} reason={}",
        execution.id(),
        event.result(),
        rejected,
        reason);
  }

  private static WorkflowResult result(Status status) {
    return switch (status) {
      case FINISHED -> WorkflowResult.FINISHED;
      case FAILED -> WorkflowResult.FAILED;
      case ABORTED -> WorkflowResult.ABORTED;
      default -> throw new IllegalArgumentException("Not a terminal workflow status: " + status);
    };
  }

  private static boolean isTerminal(Status status) {
    return status == Status.FINISHED || status == Status.FAILED || status == Status.ABORTED;
  }
}
