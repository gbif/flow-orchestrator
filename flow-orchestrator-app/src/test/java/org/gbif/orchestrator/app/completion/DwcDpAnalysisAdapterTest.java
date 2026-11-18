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
import org.gbif.orchestrator.api.report.ReportTypes;
import org.gbif.orchestrator.app.command.CommandService;
import org.gbif.orchestrator.app.json.ContractJson;
import org.gbif.orchestrator.app.persistence.OutboxMessage;
import org.gbif.orchestrator.app.persistence.OutboxRepository;
import org.gbif.orchestrator.app.persistence.StepAttempt;
import org.gbif.orchestrator.app.persistence.StepAttemptRepository;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class DwcDpAnalysisAdapterTest {

  private static final UUID DATASET = UUID.fromString("673f2308-3501-474f-85d5-a832825bb46c");
  private static final StepType VALIDATE = StepType.VALIDATOR_VALIDATE_DWCDP;

  @Autowired JdbcClient jdbc;
  @Autowired CommandService commands;
  @Autowired DwcDpAnalysisAdapter adapter;
  @Autowired WorkflowExecutionRepository executions;
  @Autowired StepAttemptRepository attempts;
  @Autowired OutboxRepository outbox;
  @Autowired WorkflowReportRepository reports;
  @Autowired ContractJson json;

  @BeforeEach
  void clear() {
    Database.clear(jdbc);
  }

  @Test
  void validAnalysisCompletesTheStepAndDispatchesStaging() {
    UUID id = start();

    CompletionResult result = adapter.handle(message(DATASET, 1, true));

    assertEquals(CompletionResult.PROCESSED, result);
    StepAttempt attempt = attempts.find(id, VALIDATE, 1).orElseThrow();
    assertEquals(Status.FINISHED, attempt.status());
    assertEquals(Instant.parse("2026-11-18T10:00:00.250Z"), attempt.started());
    assertEquals(Instant.parse("2026-11-18T10:00:04.500Z"), attempt.finished());

    WorkflowReport report = reports.findBySubject(DATASET, 1).get(0);
    assertEquals(ReportTypes.DWCDP_ANALYSIS, report.reportType());
    assertEquals(analysisExecution(true), report.report(), "stored unchanged");

    List<OutboxMessage> messages = outbox.findByExecution(id);
    assertEquals(StepType.VALIDATOR_DWCDP_STAGE, messages.get(messages.size() - 1).stepType());
  }

  @Test
  void invalidAnalysisRejectsTheWorkflow() {
    UUID id = start();

    adapter.handle(message(DATASET, 1, false));

    assertEquals(Status.FINISHED, executions.find(id).orElseThrow().status());
    assertTrue(executions.find(id).orElseThrow().rejected());
  }

  @Test
  void analysisWithoutMetadataFailsTheStep() {
    // What an analyser that still sends only the DatapackageAnalysisResult gives.
    UUID id = start();
    JsonNode old =
        json.read(
            "{\"datasetUuid\":\""
                + DATASET
                + "\",\"attempt\":1,\"valid\":true,"
                + "\"validationReport\":{\"resourceAnalysisResults\":[]}}",
            JsonNode.class);

    adapter.handle(old);

    StepAttempt attempt = attempts.find(id, VALIDATE, 1).orElseThrow();
    assertEquals(Status.FAILED, attempt.status());
    assertTrue(attempt.diagnostic().contains("metadata.isValid"), attempt.diagnostic());
  }

  @Test
  void redeliveryIsIgnored() {
    start();
    adapter.handle(message(DATASET, 1, true));

    assertEquals(CompletionResult.IGNORED, adapter.handle(message(DATASET, 1, true)));
  }

  @Test
  void unknownSubjectIsRefused() {
    start();

    assertThrows(
        UnknownWorkflowException.class,
        () -> adapter.handle(message(UUID.randomUUID(), 1, true)));
    assertThrows(UnknownWorkflowException.class, () -> adapter.handle(message(DATASET, 2, true)));
  }

  @Test
  void malformedMessagesAreRefused() {
    start();

    assertThrows(
        IllegalArgumentException.class,
        () -> adapter.handle(json.read("{\"attempt\":1}", JsonNode.class)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            adapter.handle(
                json.read(
                    "{\"datasetUuid\":\"" + DATASET + "\",\"attempt\":1}", JsonNode.class)));
  }

  @Test
  void analyserTimesAreReadAsUtcInBothForms() {
    Instant fallback = Instant.EPOCH;
    Instant expected = Instant.parse("2026-11-18T10:00:00.250Z");

    assertEquals(
        expected,
        DwcDpAnalysisAdapter.instant(json.read("\"2026-11-18T10:00:00.25\"", JsonNode.class), fallback));
    assertEquals(
        expected,
        DwcDpAnalysisAdapter.instant(
            json.read("\"2026-11-18T10:00:00.250+00:00\"", JsonNode.class), fallback));
    assertEquals(
        fallback,
        DwcDpAnalysisAdapter.instant(json.read("\"yesterday\"", JsonNode.class), fallback));
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

  private JsonNode analysisExecution(boolean valid) {
    return json.read(
        "{\"result\":{\"resourceAnalysisResults\":[]},"
            + "\"metadata\":{\"started\":\"2026-11-18T10:00:00.25\","
            + "\"finished\":\"2026-11-18T10:00:04.5\",\"features\":[],\"isValid\":"
            + valid
            + "},\"version\":\"1.0\"}",
        JsonNode.class);
  }

  /** {@code DwcDpValidationFinished} after the analyser change (validationReport = execution). */
  private JsonNode message(UUID dataset, int attempt, boolean valid) {
    return json.read(
        "{\"datasetUuid\":\""
            + dataset
            + "\",\"attempt\":"
            + attempt
            + ",\"valid\":"
            + valid
            + ",\"validationReport\":"
            + analysisExecution(valid)
            + "}",
        JsonNode.class);
  }
}
