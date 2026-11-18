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
import org.gbif.orchestrator.api.command.CommandType;
import org.gbif.orchestrator.api.command.Domain;
import org.gbif.orchestrator.api.command.WorkflowCommand;
import org.gbif.orchestrator.app.json.ContractJson;
import org.gbif.orchestrator.app.persistence.CommandOutcome;
import org.gbif.orchestrator.app.persistence.CommandRecord;
import org.gbif.orchestrator.app.persistence.CommandRepository;
import org.gbif.orchestrator.app.persistence.OutboxMessage;
import org.gbif.orchestrator.app.persistence.OutboxRepository;
import org.gbif.orchestrator.app.persistence.StepAttempt;
import org.gbif.orchestrator.app.persistence.StepAttemptRepository;
import org.gbif.orchestrator.app.persistence.StepExecution;
import org.gbif.orchestrator.app.persistence.StepExecutionRepository;
import org.gbif.orchestrator.app.persistence.WorkflowExecution;
import org.gbif.orchestrator.app.persistence.WorkflowExecutionRepository;
import org.gbif.orchestrator.app.support.Database;
import org.gbif.orchestrator.engine.WorkflowState.Status;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.fasterxml.jackson.databind.JsonNode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * START handling against H2, using the test workflow {@code validation-dwcdp}: VALIDATE_DWCDP →
 * DWCDP_STAGE → DWCDP_TO_VERBATIM (see test {@code config/application.yml}).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class CommandServiceTest {

  private static final UUID DATASET = UUID.fromString("673f2308-3501-474f-85d5-a832825bb46c");

  @Autowired JdbcClient jdbc;
  @Autowired CommandService service;
  @Autowired CommandRepository commands;
  @Autowired WorkflowExecutionRepository executions;
  @Autowired StepExecutionRepository steps;
  @Autowired StepAttemptRepository attempts;
  @Autowired OutboxRepository outbox;
  @Autowired ContractJson json;

  @BeforeEach
  void clear() {
    Database.clear(jdbc);
  }

  @Test
  void startCreatesStateFirstAttemptAndOutboxRow() {
    WorkflowCommand command = start(Domain.VALIDATION, Map.of("fileFormat", "DWCDP"));

    CommandResult result = service.handle(command);

    assertEquals(CommandResult.Kind.ACCEPTED, result.kind());
    UUID id = result.workflowExecutionId();

    WorkflowExecution execution = executions.find(id).orElseThrow();
    assertEquals(Status.RUNNING, execution.status());
    assertEquals(1, execution.version());
    assertEquals("validation-dwcdp", execution.workflowName());
    assertEquals(Map.of("fileFormat", "DWCDP"), execution.parameters());
    assertNull(execution.finished());

    List<StepExecution> stepRows = steps.findByExecution(id);
    assertEquals(
        List.of(
            StepType.VALIDATOR_VALIDATE_DWCDP,
            StepType.VALIDATOR_DWCDP_STAGE,
            StepType.VALIDATOR_DWCDP_TO_VERBATIM),
        stepRows.stream().map(StepExecution::stepType).toList());
    assertEquals(Status.QUEUED, stepRows.get(0).status());
    assertEquals(1, stepRows.get(0).currentAttempt());
    assertEquals(Status.PENDING, stepRows.get(1).status());
    assertEquals(0, stepRows.get(1).currentAttempt());

    List<StepAttempt> attemptRows = attempts.findByExecution(id);
    assertEquals(1, attemptRows.size());
    assertEquals(StepType.VALIDATOR_VALIDATE_DWCDP, attemptRows.get(0).stepType());
    assertEquals(1, attemptRows.get(0).attemptNo());
    assertEquals(Status.QUEUED, attemptRows.get(0).status());

    List<OutboxMessage> messages = outbox.findByExecution(id);
    assertEquals(1, messages.size());
    OutboxMessage message = messages.get(0);
    assertEquals("occurrence", message.exchange());
    assertEquals("validator.dwcdp.analysis", message.routingKey());
    assertEquals(StepType.VALIDATOR_VALIDATE_DWCDP, message.stepType());
    assertEquals(Integer.valueOf(1), message.stepAttempt());
    assertNull(message.published());

    // The analyser keeps its existing request contract.
    JsonNode payload = json.read(message.payload(), JsonNode.class);
    assertEquals(2, payload.size());
    assertEquals(DATASET.toString(), payload.get("datasetUuid").asText());
    assertEquals(1, payload.get("attempt").asInt());

    CommandRecord record = commands.find(command.commandId()).orElseThrow();
    assertEquals(CommandOutcome.ACCEPTED, record.outcome());
    assertEquals(id, record.workflowExecutionId());
  }

  @Test
  void duplicateCommandIsANoOp() {
    WorkflowCommand command = start(Domain.VALIDATION, Map.of("fileFormat", "DWCDP"));
    service.handle(command);

    CommandResult again = service.handle(command);

    assertEquals(CommandResult.Kind.DUPLICATE, again.kind());
    assertEquals(1, Database.count(jdbc, "workflow_execution"));
    assertEquals(1, Database.count(jdbc, "outbox_message"));
    assertEquals(1, Database.count(jdbc, "workflow_command"));
  }

  @Test
  void secondStartForSameSubjectIsRejected() {
    service.handle(start(Domain.VALIDATION, Map.of("fileFormat", "DWCDP")));
    WorkflowCommand second = start(Domain.VALIDATION, Map.of("fileFormat", "DWCDP"));

    CommandResult result = service.handle(second);

    assertRejected(second, result, "already started");
    assertEquals(1, Database.count(jdbc, "workflow_execution"));
  }

  @Test
  void otherDomainIsRejected() {
    WorkflowCommand command = start(Domain.INGESTION, Map.of("fileFormat", "DWCDP"));
    assertRejected(command, service.handle(command), "not handled by this instance");
  }

  @Test
  void unmatchedParametersAreRejected() {
    WorkflowCommand command = start(Domain.VALIDATION, Map.of("fileFormat", "XML"));
    assertRejected(command, service.handle(command), "No single workflow matches");
  }

  @Test
  void otherCommandTypesAreRejectedForNow() {
    WorkflowCommand retry =
        new WorkflowCommand(
            UUID.randomUUID(),
            CommandType.RETRY_STEP,
            null,
            DATASET,
            1,
            StepType.VALIDATOR_VALIDATE_DWCDP,
            null,
            "jdoe",
            null,
            Instant.now());
    assertRejected(retry, service.handle(retry), "not supported yet");
  }

  private void assertRejected(WorkflowCommand command, CommandResult result, String reason) {
    assertEquals(CommandResult.Kind.REJECTED, result.kind());
    assertTrue(result.reason().contains(reason), result.reason());
    CommandRecord record = commands.find(command.commandId()).orElseThrow();
    assertEquals(CommandOutcome.REJECTED, record.outcome());
    assertEquals(result.reason(), record.outcomeReason());
    assertNull(record.workflowExecutionId());
  }

  static WorkflowCommand start(Domain domain, Map<String, String> parameters) {
    return new WorkflowCommand(
        UUID.randomUUID(),
        CommandType.START,
        domain,
        DATASET,
        1,
        null,
        parameters,
        "jdoe",
        null,
        Instant.now());
  }
}
