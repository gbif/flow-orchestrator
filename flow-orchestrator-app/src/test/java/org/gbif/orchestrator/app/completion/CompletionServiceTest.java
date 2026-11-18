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
import org.gbif.orchestrator.api.command.CommandType;
import org.gbif.orchestrator.api.command.Domain;
import org.gbif.orchestrator.api.command.WorkflowCommand;
import org.gbif.orchestrator.api.event.WorkflowFinished;
import org.gbif.orchestrator.api.event.WorkflowResult;
import org.gbif.orchestrator.api.report.ReportTypes;
import org.gbif.orchestrator.api.step.Outcome;
import org.gbif.orchestrator.api.step.Report;
import org.gbif.orchestrator.api.step.StepCompleted;
import org.gbif.orchestrator.api.step.StepHeader;
import org.gbif.orchestrator.app.command.CommandService;
import org.gbif.orchestrator.app.json.ContractJson;
import org.gbif.orchestrator.app.messaging.CompletionListener;
import org.gbif.orchestrator.app.persistence.OutboxMessage;
import org.gbif.orchestrator.app.persistence.OutboxRepository;
import org.gbif.orchestrator.app.persistence.StepAttempt;
import org.gbif.orchestrator.app.persistence.StepAttemptRepository;
import org.gbif.orchestrator.app.persistence.StepExecution;
import org.gbif.orchestrator.app.persistence.StepExecutionRepository;
import org.gbif.orchestrator.app.persistence.WorkflowExecution;
import org.gbif.orchestrator.app.persistence.WorkflowExecutionRepository;
import org.gbif.orchestrator.app.persistence.WorkflowReport;
import org.gbif.orchestrator.app.persistence.WorkflowReportRepository;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Step completions against the test workflow VALIDATE_DWCDP → DWCDP_STAGE → DWCDP_TO_VERBATIM. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class CompletionServiceTest {

  private static final UUID DATASET = UUID.fromString("673f2308-3501-474f-85d5-a832825bb46c");
  private static final StepType VALIDATE = StepType.VALIDATOR_VALIDATE_DWCDP;
  private static final StepType STAGE = StepType.VALIDATOR_DWCDP_STAGE;
  private static final StepType TO_VERBATIM = StepType.VALIDATOR_DWCDP_TO_VERBATIM;
  private static final Instant STARTED = Instant.parse("2026-11-16T12:00:05Z");
  private static final Instant FINISHED = Instant.parse("2026-11-16T12:01:10Z");

  @Autowired JdbcClient jdbc;
  @Autowired CommandService commands;
  @Autowired CompletionService service;
  @Autowired WorkflowExecutionRepository executions;
  @Autowired StepExecutionRepository steps;
  @Autowired StepAttemptRepository attempts;
  @Autowired OutboxRepository outbox;
  @Autowired WorkflowReportRepository reports;
  @Autowired ContractJson json;

  @BeforeEach
  void clear() {
    Database.clear(jdbc);
  }

  @Test
  void successRecordsTheAttemptAndReportAndDispatchesTheNextStep() {
    UUID id = start();

    CompletionResult result =
        service.handle(
            completed(
                id, VALIDATE, 1, Outcome.SUCCEEDED, null, List.of(analysis(true, null))));

    assertEquals(CompletionResult.PROCESSED, result);
    StepAttempt attempt = attempts.find(id, VALIDATE, 1).orElseThrow();
    assertEquals(Status.FINISHED, attempt.status());
    assertEquals(Outcome.SUCCEEDED, attempt.outcome());
    assertEquals(Map.of("datapackagePath", "/data/1"), attempt.outputs());
    assertEquals(STARTED, attempt.started());
    assertEquals(FINISHED, attempt.finished());

    assertEquals(
        List.of(Status.FINISHED, Status.QUEUED, Status.PENDING),
        steps.findByExecution(id).stream().map(StepExecution::status).toList());
    assertEquals(Status.QUEUED, attempts.find(id, STAGE, 1).orElseThrow().status());

    List<OutboxMessage> messages = outbox.findByExecution(id);
    assertEquals(2, messages.size());
    assertEquals("validator.dwcdp.stage", messages.get(1).routingKey());
    assertEquals(STAGE, messages.get(1).stepType());

    WorkflowExecution execution = executions.find(id).orElseThrow();
    assertEquals(Status.RUNNING, execution.status());
    assertEquals(2, execution.version());

    WorkflowReport report = reports.findBySubject(DATASET, 1).get(0);
    assertEquals(ReportTypes.DWCDP_ANALYSIS, report.reportType());
    assertTrue(report.report().path("metadata").path("isValid").asBoolean());
    assertEquals("VALIDATOR_VALIDATE_DWCDP attempt 1", report.comment());
  }

  @Test
  void reportKeepsItsOwnComment() {
    UUID id = start();

    service.handle(
        completed(
            id, VALIDATE, 1, Outcome.SUCCEEDED, null, List.of(analysis(true, "from the analyser"))));

    assertEquals("from the analyser", reports.findBySubject(DATASET, 1).get(0).comment());
  }

  @Test
  void lastStepFinishesTheWorkflowAndWritesWorkflowFinished() {
    UUID id = start();
    service.handle(
        completed(id, VALIDATE, 1, Outcome.SUCCEEDED, null, List.of(analysis(true, null))));
    service.handle(completed(id, STAGE, 1, Outcome.SUCCEEDED, null, List.of()));

    service.handle(completed(id, TO_VERBATIM, 1, Outcome.SUCCEEDED, null, List.of()));

    WorkflowExecution execution = executions.find(id).orElseThrow();
    assertEquals(Status.FINISHED, execution.status());
    assertNotNull(execution.finished());
    assertFalse(execution.rejected());
    assertNull(execution.reason());

    OutboxMessage last = lastMessage(id);
    assertNull(last.stepType());
    assertEquals("orchestrator", last.exchange());
    assertEquals("validation.workflow.finished", last.routingKey());
    WorkflowFinished event = json.read(last.payload(), WorkflowFinished.class);
    assertEquals(id, event.workflowId());
    assertEquals(Domain.VALIDATION, event.domain());
    assertEquals(DATASET, event.datasetKey());
    assertEquals(WorkflowResult.FINISHED, event.result());
    assertFalse(event.rejected());
    assertEquals(List.of(ReportTypes.DWCDP_ANALYSIS), event.reportTypes());
  }

  @Test
  void failureFailsTheWorkflowAndStoresReasons() {
    UUID id = start();

    service.handle(completed(id, VALIDATE, 1, Outcome.FAILED, "Archive not found", List.of()));

    StepAttempt attempt = attempts.find(id, VALIDATE, 1).orElseThrow();
    assertEquals(Status.FAILED, attempt.status());
    assertEquals("Archive not found", attempt.diagnostic());

    List<StepExecution> stepRows = steps.findByExecution(id);
    assertEquals(Status.FAILED, stepRows.get(0).status());
    assertEquals("Archive not found", stepRows.get(0).reason());
    assertEquals(Status.ABORTED, stepRows.get(1).status());
    assertEquals("Not run: VALIDATOR_VALIDATE_DWCDP failed", stepRows.get(1).reason());
    assertEquals(Status.ABORTED, stepRows.get(2).status());

    WorkflowExecution execution = executions.find(id).orElseThrow();
    assertEquals(Status.FAILED, execution.status());
    assertEquals("VALIDATOR_VALIDATE_DWCDP failed: Archive not found", execution.reason());

    WorkflowFinished event = json.read(lastMessage(id).payload(), WorkflowFinished.class);
    assertEquals(WorkflowResult.FAILED, event.result());
    assertFalse(event.rejected());
    assertEquals("VALIDATOR_VALIDATE_DWCDP failed: Archive not found", event.reason());
    assertEquals(2, outbox.findByExecution(id).size(), "first dispatch and WorkflowFinished");
  }

  @Test
  void invalidAnalysisSkipsTheRestAndFinishesRejected() {
    UUID id = start();

    service.handle(
        completed(id, VALIDATE, 1, Outcome.SUCCEEDED, null, List.of(analysis(false, null))));

    String reason = "The DwC-DP analysis found the data package invalid";
    StepAttempt attempt = attempts.find(id, VALIDATE, 1).orElseThrow();
    assertEquals(Status.FINISHED, attempt.status(), "a rejection is not a step failure");

    List<StepExecution> stepRows = steps.findByExecution(id);
    assertEquals(Status.FINISHED, stepRows.get(0).status());
    assertNull(stepRows.get(0).reason());
    assertEquals(Status.SKIPPED, stepRows.get(1).status());
    assertEquals("Not run: " + reason, stepRows.get(1).reason());
    assertEquals(Status.SKIPPED, stepRows.get(2).status());

    WorkflowExecution execution = executions.find(id).orElseThrow();
    assertEquals(Status.FINISHED, execution.status());
    assertTrue(execution.rejected());
    assertEquals(reason, execution.reason());

    WorkflowFinished event = json.read(lastMessage(id).payload(), WorkflowFinished.class);
    assertEquals(WorkflowResult.FINISHED, event.result());
    assertTrue(event.rejected());
    assertEquals(reason, event.reason());
    assertEquals(List.of(ReportTypes.DWCDP_ANALYSIS), event.reportTypes());
    assertEquals(2, outbox.findByExecution(id).size(), "no dispatch after the rejection");
  }

  @Test
  void unreadableReportFailsTheStepAndKeepsTheReport() {
    UUID id = start();
    Report noVerdict =
        new Report(
            ReportTypes.DWCDP_ANALYSIS, json.read("{\"result\":{}}", JsonNode.class), null);

    service.handle(completed(id, VALIDATE, 1, Outcome.SUCCEEDED, null, List.of(noVerdict)));

    StepAttempt attempt = attempts.find(id, VALIDATE, 1).orElseThrow();
    assertEquals(Status.FAILED, attempt.status());
    assertEquals(Outcome.SUCCEEDED, attempt.outcome(), "what the worker said is kept");
    assertEquals(
        "Unreadable report: DWCDP_ANALYSIS report has no boolean metadata.isValid",
        attempt.diagnostic());

    WorkflowExecution execution = executions.find(id).orElseThrow();
    assertEquals(Status.FAILED, execution.status());
    assertFalse(execution.rejected());
    assertEquals(1, reports.findBySubject(DATASET, 1).size(), "the unreadable report is kept");
  }

  @Test
  void missingReportFailsTheStep() {
    UUID id = start();

    service.handle(completed(id, VALIDATE, 1, Outcome.SUCCEEDED, null, List.of()));

    assertEquals(
        "Unreadable report: Missing DWCDP_ANALYSIS report (a JSON object)",
        attempts.find(id, VALIDATE, 1).orElseThrow().diagnostic());
    assertEquals(Status.FAILED, executions.find(id).orElseThrow().status());
  }

  @Test
  void redeliveredAndStaleCompletionsAreIgnored() {
    UUID id = start();
    StepCompleted completion =
        completed(id, VALIDATE, 1, Outcome.SUCCEEDED, null, List.of(analysis(true, null)));
    service.handle(completion);
    WorkflowExecution before = executions.find(id).orElseThrow();

    assertEquals(CompletionResult.IGNORED, service.handle(completion), "redelivery");
    assertEquals(
        CompletionResult.IGNORED,
        service.handle(completed(id, STAGE, 2, Outcome.SUCCEEDED, null, List.of())),
        "not the current attempt");
    assertEquals(
        CompletionResult.IGNORED,
        service.handle(completed(id, TO_VERBATIM, 1, Outcome.SUCCEEDED, null, List.of())),
        "never dispatched");

    assertEquals(before, executions.find(id).orElseThrow());
    assertEquals(2, Database.count(jdbc, "outbox_message"));
    assertEquals(2, Database.count(jdbc, "step_attempt"));
  }

  @Test
  void completionsThatCannotBelongHereAreRefused() {
    UUID id = start();

    assertThrows(
        UnknownWorkflowException.class,
        () ->
            service.handle(
                completed(UUID.randomUUID(), VALIDATE, 1, Outcome.SUCCEEDED, null, List.of())));
    assertThrows(
        UnknownWorkflowException.class,
        () ->
            service.handle(
                completed(
                    id,
                    StepType.VALIDATOR_COLLECT_METRICS,
                    1,
                    Outcome.SUCCEEDED,
                    null,
                    List.of())));
    StepCompleted otherDataset =
        new StepCompleted(
            UUID.randomUUID(),
            new StepHeader(id, UUID.randomUUID(), 1, VALIDATE, 1),
            Outcome.SUCCEEDED,
            null,
            null,
            null,
            STARTED,
            FINISHED);
    assertThrows(UnknownWorkflowException.class, () -> service.handle(otherDataset));

    jdbc.sql("UPDATE workflow_execution SET domain = 'INGESTION' WHERE id = :id")
        .param("id", id)
        .update();
    assertThrows(
        UnknownWorkflowException.class,
        () -> service.handle(completed(id, VALIDATE, 1, Outcome.SUCCEEDED, null, List.of())));

    // the listener lets the exception through, so postal dead-letters the message
    CompletionListener listener = new CompletionListener(service);
    assertEquals(StepCompleted.class, listener.getMessageClass());
    assertThrows(
        UnknownWorkflowException.class,
        () ->
            listener.handleMessage(
                completed(id, VALIDATE, 1, Outcome.SUCCEEDED, null, List.of())));
  }

  @Test
  void changedDefinitionIsRefused() {
    UUID id = start();
    jdbc.sql("UPDATE workflow_execution SET definition_hash = 'other' WHERE id = :id")
        .param("id", id)
        .update();

    assertThrows(
        IllegalStateException.class,
        () -> service.handle(completed(id, VALIDATE, 1, Outcome.SUCCEEDED, null, List.of())));
    assertEquals(Status.QUEUED, steps.findByExecution(id).get(0).status(), "rolled back");
  }

  private UUID start() {
    return commands
        .handle(
            new WorkflowCommand(
                UUID.randomUUID(),
                CommandType.START,
                Domain.VALIDATION,
                DATASET,
                1,
                null,
                Map.of("fileFormat", "DWCDP"),
                "jdoe",
                null,
                Instant.now()))
        .workflowExecutionId();
  }

  private OutboxMessage lastMessage(UUID id) {
    List<OutboxMessage> messages = outbox.findByExecution(id);
    return messages.get(messages.size() - 1);
  }

  static Report analysis(boolean valid, String comment) {
    // The analyser's AnalysisExecution: {result, metadata: {..., isValid}, version}
    JsonNode content =
        new ContractJson().read(
            "{\"result\":{\"resourceAnalysisResults\":[]},\"metadata\":{\"isValid\":"
                + valid
                + "},\"version\":\"1.0\"}",
            JsonNode.class);
    return new Report(ReportTypes.DWCDP_ANALYSIS, content, comment);
  }

  static StepCompleted completed(
      UUID workflowId,
      StepType step,
      int stepAttempt,
      Outcome outcome,
      String diagnostic,
      List<Report> reports) {
    return new StepCompleted(
        UUID.randomUUID(),
        new StepHeader(workflowId, DATASET, 1, step, stepAttempt),
        outcome,
        diagnostic,
        outcome == Outcome.SUCCEEDED ? Map.of("datapackagePath", "/data/1") : null,
        reports,
        STARTED,
        FINISHED);
  }
}
