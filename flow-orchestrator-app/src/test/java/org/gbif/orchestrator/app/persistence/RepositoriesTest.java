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
package org.gbif.orchestrator.app.persistence;

import org.gbif.api.model.pipelines.StepType;
import org.gbif.orchestrator.api.command.CommandType;
import org.gbif.orchestrator.api.command.Domain;
import org.gbif.orchestrator.api.command.WorkflowCommand;
import org.gbif.orchestrator.api.step.Outcome;
import org.gbif.orchestrator.app.json.ContractJson;
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
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.fasterxml.jackson.databind.JsonNode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Round trips through every repository against the orchestrator table script on H2. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class RepositoriesTest {

  private static final Instant T0 = Instant.parse("2026-11-16T12:00:00Z");
  private static final Instant T1 = Instant.parse("2026-11-16T12:05:00Z");
  private static final Instant T2 = Instant.parse("2026-11-16T12:10:00Z");
  private static final UUID DATASET = UUID.fromString("673f2308-3501-474f-85d5-a832825bb46c");
  private static final StepType STEP = StepType.VALIDATOR_VALIDATE_DWCDP;

  @Autowired JdbcClient jdbc;
  @Autowired WorkflowExecutionRepository executions;
  @Autowired StepExecutionRepository steps;
  @Autowired StepAttemptRepository attempts;
  @Autowired CommandRepository commands;
  @Autowired OutboxRepository outbox;
  @Autowired WorkflowReportRepository reports;
  @Autowired WorkflowStateLoader loader;
  @Autowired ContractJson json;

  @BeforeEach
  void clear() {
    Database.clear(jdbc);
  }

  @Test
  void workflowExecutionRoundTripAndVersionCheck() {
    WorkflowExecution execution = execution(UUID.randomUUID(), Domain.VALIDATION);
    executions.insert(execution);

    assertEquals(execution, executions.find(execution.id()).orElseThrow());
    assertTrue(executions.exists(Domain.VALIDATION, DATASET, 1, 1));
    assertFalse(executions.exists(Domain.VALIDATION, DATASET, 1, 2));
    assertFalse(executions.exists(Domain.INGESTION, DATASET, 1, 1));

    assertTrue(
        executions.updateIfVersion(
            execution.id(), 0, Status.FINISHED, T1, T1, true, "data package is invalid"));
    assertFalse(
        executions.updateIfVersion(execution.id(), 0, Status.FAILED, T1, T1, false, null),
        "stale version");

    WorkflowExecution updated = executions.find(execution.id()).orElseThrow();
    assertEquals(1, updated.version());
    assertEquals(Status.FINISHED, updated.status());
    assertEquals(T1, updated.finished());
    assertTrue(updated.rejected());
    assertEquals("data package is invalid", updated.reason());
  }

  @Test
  void subjectRunIsUnique() {
    executions.insert(execution(UUID.randomUUID(), Domain.VALIDATION));
    assertThrows(
        DuplicateKeyException.class,
        () -> executions.insert(execution(UUID.randomUUID(), Domain.VALIDATION)));
  }

  @Test
  void stepsAndAttemptsRoundTripAndLoadAsState() {
    WorkflowExecution execution = execution(UUID.randomUUID(), Domain.VALIDATION);
    executions.insert(execution);
    StepExecution step = new StepExecution(execution.id(), STEP, 1, Status.PENDING, 0, T0, null);
    steps.insert(step);
    steps.insert(
        new StepExecution(
            execution.id(), StepType.VALIDATOR_DWCDP_STAGE, 2, Status.PENDING, 0, T0, null));
    assertEquals(step, steps.findByExecution(execution.id()).get(0));

    StepAttempt queued =
        new StepAttempt(execution.id(), STEP, 1, Status.QUEUED, null, null, null, T0, null, null);
    attempts.insert(queued);
    assertEquals(queued, attempts.find(execution.id(), STEP, 1).orElseThrow());
    assertThrows(DuplicateKeyException.class, () -> attempts.insert(queued));

    attempts.complete(
        execution.id(),
        STEP,
        1,
        Status.FAILED,
        Outcome.FAILED,
        "Archive not found",
        Map.of("path", "/tmp/x"),
        T1,
        T2);
    steps.update(execution.id(), STEP, Status.FAILED, 1, T2, "Archive not found");

    StepAttempt completed =
        new StepAttempt(
            execution.id(),
            STEP,
            1,
            Status.FAILED,
            Outcome.FAILED,
            "Archive not found",
            Map.of("path", "/tmp/x"),
            T0,
            T1,
            T2);
    assertEquals(completed, attempts.find(execution.id(), STEP, 1).orElseThrow());
    assertEquals(List.of(completed), attempts.findByExecution(execution.id()));
    assertThrows(
        IllegalStateException.class,
        () ->
            attempts.complete(
                execution.id(), STEP, 2, Status.FINISHED, Outcome.SUCCEEDED, null, null, T1, T2));

    WorkflowStateLoader.Loaded loaded = loader.load(execution.id());
    assertEquals(execution, loaded.execution());
    assertEquals(Status.FAILED, loaded.state().steps().get(0).status());
    assertEquals("Archive not found", loaded.steps().get(0).reason());
    assertEquals(2, loaded.state().steps().get(1).order());
    assertTrue(loader.find(UUID.randomUUID()).isEmpty());
  }

  @Test
  void commandRoundTrip() {
    WorkflowCommand command =
        new WorkflowCommand(
            UUID.randomUUID(),
            CommandType.RETRY_STEP,
            null,
            DATASET,
            1,
            STEP,
            Map.of("a", "b"),
            "jdoe",
            "because",
            T0);
    CommandRecord record =
        new CommandRecord(command, T1, null, CommandOutcome.REJECTED, "not supported");

    assertFalse(commands.exists(command.commandId()));
    commands.insert(record);
    assertTrue(commands.exists(command.commandId()));
    assertEquals(record, commands.find(command.commandId()).orElseThrow());
    assertThrows(DuplicateKeyException.class, () -> commands.insert(record));
  }

  @Test
  void outboxRoundTrip() {
    WorkflowExecution execution = execution(UUID.randomUUID(), Domain.VALIDATION);
    executions.insert(execution);
    OutboxMessage message = message(execution.id(), STEP, 1, null);
    outbox.insert(message);
    outbox.insert(message(execution.id(), null, null, null));

    List<OutboxMessage> stored = outbox.findByExecution(execution.id());
    assertEquals(2, stored.size());
    OutboxMessage first = stored.get(0);
    assertNotNull(first.id());
    assertEquals(message.messageId(), first.messageId());
    assertEquals(Integer.valueOf(1), first.stepAttempt());
    assertEquals("{\"x\":1}", first.payload());
    assertNull(first.published());
    assertNull(first.nextAttempt());
    assertNull(stored.get(1).stepAttempt());
    assertTrue(stored.get(1).id() > first.id());
  }

  @Test
  void outboxDueRowsArePerDomainOrderedAndRespectBackoff() {
    WorkflowExecution validation = execution(UUID.randomUUID(), Domain.VALIDATION);
    WorkflowExecution ingestion =
        new WorkflowExecution(
            UUID.randomUUID(),
            Domain.INGESTION,
            DATASET,
            1,
            1,
            "ingestion",
            "abc",
            Map.of(),
            Status.PENDING,
            0,
            T0,
            T0,
            null,
            false,
            null);
    executions.insert(validation);
    executions.insert(ingestion);
    outbox.insert(message(validation.id(), STEP, 1, null));
    outbox.insert(message(ingestion.id(), STEP, 1, null));
    outbox.insert(message(validation.id(), null, null, null));

    List<OutboxMessage> due = outbox.findDue(Domain.VALIDATION, T1, 10);
    assertEquals(2, due.size(), "only the instance's domain");
    assertTrue(due.get(0).id() < due.get(1).id());
    assertEquals(1, outbox.findDue(Domain.VALIDATION, T1, 1).size(), "batch limit");
    assertEquals(new OutboxRepository.Backlog(2, T0), outbox.backlog(Domain.VALIDATION));

    outbox.markFailed(due.get(0).id(), 1, "unroutable", T2);
    outbox.markPublished(due.get(1).id(), T1);

    assertTrue(outbox.findDue(Domain.VALIDATION, T1, 10).isEmpty(), "backing off, or published");
    List<OutboxMessage> later = outbox.findDue(Domain.VALIDATION, T2, 10);
    assertEquals(1, later.size());
    assertEquals(1, later.get(0).publishAttempts());
    assertEquals("unroutable", later.get(0).lastError());
    assertEquals(T2, later.get(0).nextAttempt());
    assertEquals(1, outbox.backlog(Domain.VALIDATION).unpublished());
    assertEquals(new OutboxRepository.Backlog(1, T0), outbox.backlog(Domain.INGESTION));
  }

  @Test
  void reportsUpsertKeepCreatedAndAreJson() {
    JsonNode first = json.read("{\"valid\":false,\"issues\":[\"a\"]}", JsonNode.class);
    JsonNode second = json.read("{\"valid\":true}", JsonNode.class);
    reports.upsert(new WorkflowReport(DATASET, 1, "DWCDP_ANALYSIS", first, "attempt 1", T0, T0));
    reports.upsert(new WorkflowReport(DATASET, 1, "CHECKLIST", second, null, T0, T0));
    reports.upsert(new WorkflowReport(DATASET, 2, "CHECKLIST", second, null, T0, T0));

    assertEquals(
        List.of(
            new WorkflowReport(DATASET, 1, "CHECKLIST", second, null, T0, T0),
            new WorkflowReport(DATASET, 1, "DWCDP_ANALYSIS", first, "attempt 1", T0, T0)),
        reports.findBySubject(DATASET, 1));

    reports.upsert(new WorkflowReport(DATASET, 1, "DWCDP_ANALYSIS", second, "attempt 2", T1, T1));

    assertEquals(
        new WorkflowReport(DATASET, 1, "DWCDP_ANALYSIS", second, "attempt 2", T0, T1),
        reports.findBySubject(DATASET, 1).get(1));
    assertEquals(List.of("CHECKLIST", "DWCDP_ANALYSIS"), reports.findTypes(DATASET, 1));
    assertTrue(
        reports.findBySubject(DATASET, 1).get(1).report().get("valid").asBoolean(),
        "stored as a JSON object, not a JSON string");
  }

  private static OutboxMessage message(
      UUID executionId, StepType step, Integer stepAttempt, Instant nextAttempt) {
    return new OutboxMessage(
        null,
        UUID.randomUUID(),
        executionId,
        step,
        stepAttempt,
        "occurrence",
        "validator.dwcdp.analysis",
        "{\"x\":1}",
        T0,
        null,
        0,
        null,
        nextAttempt);
  }

  private static WorkflowExecution execution(UUID id, Domain domain) {
    return new WorkflowExecution(
        id,
        domain,
        DATASET,
        1,
        1,
        "validation-dwcdp",
        "abc",
        Map.of("fileFormat", "DWCDP"),
        Status.PENDING,
        0,
        T0,
        T0,
        null,
        false,
        null);
  }
}
