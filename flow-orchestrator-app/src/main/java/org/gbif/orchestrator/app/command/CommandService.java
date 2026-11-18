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
import org.gbif.orchestrator.api.command.WorkflowCommand;
import org.gbif.orchestrator.app.config.OrchestratorProperties;
import org.gbif.orchestrator.app.config.WorkflowDefinition;
import org.gbif.orchestrator.app.config.WorkflowDefinitions;
import org.gbif.orchestrator.app.persistence.CommandOutcome;
import org.gbif.orchestrator.app.persistence.CommandRecord;
import org.gbif.orchestrator.app.persistence.CommandRepository;
import org.gbif.orchestrator.app.persistence.RetryingTransactions;
import org.gbif.orchestrator.app.persistence.StepExecution;
import org.gbif.orchestrator.app.persistence.StepExecutionRepository;
import org.gbif.orchestrator.app.persistence.WorkflowExecution;
import org.gbif.orchestrator.app.persistence.WorkflowExecutionRepository;
import org.gbif.orchestrator.app.persistence.WorkflowStateLoader;
import org.gbif.orchestrator.engine.DagWorkflowEngine;
import org.gbif.orchestrator.engine.WorkflowDecision;
import org.gbif.orchestrator.engine.WorkflowEvent;
import org.gbif.orchestrator.engine.WorkflowState.Status;

import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Handles {@link WorkflowCommand}s. Each command is processed in one transaction that records the
 * command with its outcome and, when accepted, all resulting state and outbox rows.
 *
 * <p>Concurrency is optimistic: when another transaction changed the same workflow, or created the
 * same execution, the transaction rolls back and the command is handled again from a fresh read,
 * up to {@link #MAX_TRIES} times.
 */
@Service
public class CommandService {

  static final int MAX_TRIES = RetryingTransactions.MAX_TRIES;

  private static final Logger LOG = LoggerFactory.getLogger(CommandService.class);

  private final RetryingTransactions transactions;
  private final CommandRepository commands;
  private final WorkflowExecutionRepository executions;
  private final StepExecutionRepository steps;
  private final WorkflowStateLoader loader;
  private final DecisionApplier applier;
  private final WorkflowDefinitions definitions;
  private final OrchestratorProperties properties;
  private final Clock clock;

  public CommandService(
      RetryingTransactions transactions,
      CommandRepository commands,
      WorkflowExecutionRepository executions,
      StepExecutionRepository steps,
      WorkflowStateLoader loader,
      DecisionApplier applier,
      WorkflowDefinitions definitions,
      OrchestratorProperties properties,
      Clock clock) {
    this.transactions = transactions;
    this.commands = commands;
    this.executions = executions;
    this.steps = steps;
    this.loader = loader;
    this.applier = applier;
    this.definitions = definitions;
    this.properties = properties;
    this.clock = clock;
  }

  public CommandResult handle(WorkflowCommand command) {
    CommandResult result =
        transactions.execute("commandId=" + command.commandId(), () -> handleOnce(command));
    LOG.info(
        "Command handled commandId={} type={} dataset={} attempt={} result={} workflow={} reason={}",
        command.commandId(),
        command.type(),
        command.datasetKey(),
        command.attempt(),
        result.kind(),
        result.workflowExecutionId(),
        result.reason());
    return result;
  }

  private CommandResult handleOnce(WorkflowCommand command) {
    if (commands.exists(command.commandId())) {
      return CommandResult.duplicate();
    }
    CommandResult result =
        switch (command.type()) {
          case START -> start(command);
          case RETRY_STEP, RERUN_FROM, RESTART ->
              CommandResult.rejected(command.type() + " is not supported yet");
        };
    commands.insert(
        new CommandRecord(
            command,
            clock.instant(),
            result.workflowExecutionId(),
            result.kind() == CommandResult.Kind.ACCEPTED
                ? CommandOutcome.ACCEPTED
                : CommandOutcome.REJECTED,
            result.reason()));
    return result;
  }

  private CommandResult start(WorkflowCommand command) {
    if (command.domain() != properties.getDomain()) {
      return CommandResult.rejected(
          "Domain " + command.domain() + " is not handled by this instance");
    }
    Optional<WorkflowDefinition> resolved =
        definitions.resolve(command.domain(), command.parameters());
    if (resolved.isEmpty()) {
      return CommandResult.rejected(
          "No single workflow matches domain "
              + command.domain()
              + " and parameters "
              + command.parameters());
    }
    if (executions.exists(command.domain(), command.datasetKey(), command.attempt(), 1)) {
      return CommandResult.rejected("Workflow already started for this dataset and attempt");
    }

    WorkflowDefinition definition = resolved.get();
    Instant now = clock.instant();
    UUID id = UUID.randomUUID();
    executions.insert(
        new WorkflowExecution(
            id,
            command.domain(),
            command.datasetKey(),
            command.attempt(),
            1,
            definition.name(),
            definition.hash(),
            command.parameters(),
            Status.PENDING,
            0,
            now,
            now,
            null,
            false,
            null));
    for (StepType step : definition.order()) {
      steps.insert(
          new StepExecution(id, step, definition.orderOf(step), Status.PENDING, 0, now, null));
    }

    WorkflowStateLoader.Loaded loaded = loader.load(id);
    WorkflowDecision decision =
        new DagWorkflowEngine(definition.dag()).evaluate(loaded.state(), WorkflowEvent.start());
    applier.apply(loaded, decision);
    return CommandResult.accepted(id);
  }
}
