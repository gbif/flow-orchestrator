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
package org.gbif.orchestrator.ws.service;

import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.UserRole;
import org.gbif.common.messaging.api.messages.PipelinesChecklistValidatorMessage;
import org.gbif.orchestrator.api.json.OrchestratorJson;
import org.gbif.orchestrator.api.report.ReportTypes;
import org.gbif.orchestrator.api.step.StepHeader;
import org.gbif.orchestrator.ws.api.FileFormat;
import org.gbif.orchestrator.ws.api.Metrics;
import org.gbif.orchestrator.ws.api.Metrics.ValidationStep;
import org.gbif.orchestrator.ws.api.Validation;
import org.gbif.orchestrator.ws.api.Validation.Status;
import org.gbif.orchestrator.ws.messaging.ChecklistbankRequest;
import org.gbif.orchestrator.ws.persistence.ValidationRepository;
import org.gbif.orchestrator.ws.support.EngineRows;
import org.gbif.orchestrator.ws.support.FakeMessages;
import org.gbif.orchestrator.ws.support.RecordingEmails;
import org.gbif.orchestrator.ws.support.TestBeans;
import org.gbif.orchestrator.ws.support.Users;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.fasterxml.jackson.databind.JsonNode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round B in the ws: the ChecklistBank wait of checklist validations, and how a validation's
 * steps and files are composed from its workflow and reports (as validator-ws showed them).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@Import(TestBeans.class)
class ChecklistbankAndCompositionTest {

  private static final UUID WORKFLOW = UUID.fromString("0b6c1b5e-6a4c-4d7e-9a43-6a0f4c1f7d10");
  private static final String OCCURRENCE = "http://rs.tdwg.org/dwc/terms/Occurrence";
  private static final String SCIENTIFIC_NAME = "http://rs.tdwg.org/dwc/terms/scientificName";

  @Autowired ValidationServiceImpl service;
  @Autowired ValidationRepository validations;
  @Autowired FakeMessages messages;
  @Autowired RecordingEmails emails;
  @Autowired JdbcClient jdbc;

  @BeforeEach
  void setUp() {
    EngineRows.clear(jdbc);
    messages.clear();
    emails.sent.clear();
    Users.login("alice", UserRole.USER);
  }

  @AfterEach
  void tearDown() {
    Users.logout();
  }

  // --- the ChecklistBank wait ------------------------------------------------------------------

  @Test
  void theWaitIsRecorded() {
    UUID key = stored(Status.QUEUED, FileFormat.COLDP);

    service.awaitChecklistbank(request(key));

    Validation waiting = validations.get(key).orElseThrow();
    assertEquals(Status.WAITING_FOR_CHECKLISTBANK, waiting.getStatus());
    assertEquals(4711, waiting.getClbDatasetKey());
    assertEquals(request(key), service.checklistbankWait(waiting).orElseThrow());
  }

  @Test
  void theCallbackHandsTheImportOverWithTheStepHeader() throws Exception {
    UUID key = stored(Status.QUEUED, FileFormat.DWCA);
    service.awaitChecklistbank(request(key));

    service.validateChecklistResults(key, json("{\"datasetKey\":4711,\"status\":\"finished\"}"));

    PipelinesChecklistValidatorMessage forwarded = onlyForwarded();
    assertEquals(key, forwarded.getValidationKey());
    assertEquals(
        json("{\"datasetKey\":4711,\"status\":\"finished\"}"),
        json(forwarded.getClBResponsePayload()));
    assertEquals(request(key).header(), header(forwarded));
    // No longer waiting: the poller leaves it alone; the workflow ends it.
    assertEquals(Status.RUNNING, validations.get(key).orElseThrow().getStatus());
    assertTrue(emails.sent.isEmpty());
  }

  @Test
  void anEarlyCallbackIsKeptUntilTheStepAsksForTheWait() throws Exception {
    UUID key = stored(Status.QUEUED, FileFormat.COLDP);

    service.validateChecklistResults(key, json("{\"datasetKey\":4711,\"status\":\"finished\"}"));
    assertTrue(messages.legacy.isEmpty());

    service.awaitChecklistbank(request(key));

    PipelinesChecklistValidatorMessage forwarded = onlyForwarded();
    assertEquals(
        json("{\"datasetKey\":4711,\"status\":\"finished\"}"),
        json(forwarded.getClBResponsePayload()));
    assertEquals(request(key).header(), header(forwarded));
  }

  @Test
  void aCancelledValidationDoesNotWait() {
    UUID key = stored(Status.ABORTED, FileFormat.COLDP);

    service.awaitChecklistbank(request(key));

    Validation cancelled = validations.get(key).orElseThrow();
    assertEquals(Status.ABORTED, cancelled.getStatus());
    assertNull(cancelled.getClbDatasetKey());
  }

  // --- composition -----------------------------------------------------------------------------

  @Test
  void theChecklistBankWaitIsShownInsideTheArchiveStep() {
    UUID key = stored(Status.WAITING_FOR_CHECKLISTBANK, FileFormat.DWCA);
    UUID run = EngineRows.execution(jdbc, key, "RUNNING", false, null);
    EngineRows.step(jdbc, run, "VALIDATOR_VALIDATE_ARCHIVE", 1, "FINISHED", null);
    EngineRows.step(jdbc, run, "VALIDATOR_CHECKLISTBANK", 2, "QUEUED", null);
    EngineRows.step(jdbc, run, "VALIDATOR_DWCA_TO_VERBATIM", 3, "PENDING", null);

    Validation read = service.get(key);

    assertEquals(Status.WAITING_FOR_CHECKLISTBANK, read.getStatus());
    List<ValidationStep> steps = read.getMetrics().getStepTypes();
    assertEquals(
        List.of(
            "VALIDATOR_UPLOAD_ARCHIVE", "VALIDATOR_VALIDATE_ARCHIVE", "VALIDATOR_DWCA_TO_VERBATIM"),
        steps.stream().map(ValidationStep::getStepType).toList());
    assertEquals(
        Arrays.asList(Status.FINISHED, Status.WAITING_FOR_CHECKLISTBANK, null),
        steps.stream().map(ValidationStep::getStatus).toList());
    assertEquals(List.of(0, 1, 2), steps.stream().map(ValidationStep::getExecutionOrder).toList());
  }

  @Test
  void aFailedEvaluationShowsOnTheArchiveStep() {
    UUID key = stored(Status.RUNNING, FileFormat.COLDP);
    UUID run = EngineRows.execution(jdbc, key, "FAILED", false, "ChecklistBank import failed");
    EngineRows.step(jdbc, run, "VALIDATOR_VALIDATE_ARCHIVE", 1, "FINISHED", null);
    EngineRows.step(
        jdbc, run, "VALIDATOR_CHECKLISTBANK", 2, "FAILED", "ChecklistBank import failed");

    Validation read = service.get(key);

    assertEquals(Status.FAILED, read.getStatus());
    ValidationStep archive = read.getMetrics().getStepTypes().get(1);
    assertEquals("VALIDATOR_VALIDATE_ARCHIVE", archive.getStepType());
    assertEquals(Status.FAILED, archive.getStatus());
    assertEquals("ChecklistBank import failed", archive.getMessage());
  }

  @Test
  void archiveAndMetricsReportsAreMergedAsTheLegacyWorkersDid() {
    UUID key = stored(Status.RUNNING, FileFormat.DWCA);
    UUID run = EngineRows.execution(jdbc, key, "RUNNING", false, null);
    EngineRows.step(jdbc, run, "VALIDATOR_COLLECT_METRICS", 5, "FINISHED", null);
    EngineRows.report(
        jdbc,
        key,
        ReportTypes.ARCHIVE_VALIDATION,
        "{\"valid\":true,\"files\":["
            + "{\"fileName\":\"occurrence.txt\",\"fileType\":\"CORE\",\"count\":10,"
            + "\"rowType\":\""
            + OCCURRENCE
            + "\",\"terms\":[{\"term\":\""
            + SCIENTIFIC_NAME
            + "\",\"rawIndexed\":10}],\"issues\":[{\"issue\":\"RECORD_NOT_UNIQUELY_IDENTIFIED\","
            + "\"count\":2}]},"
            + "{\"fileName\":\"eml.xml\",\"fileType\":\"METADATA\"}]}");
    EngineRows.report(
        jdbc,
        key,
        ReportTypes.INTERPRETATION_METRICS,
        "{\"indexeable\":true,\"files\":[{\"rowType\":\""
            + OCCURRENCE
            + "\",\"indexedCount\":9,\"terms\":[{\"term\":\""
            + SCIENTIFIC_NAME
            + "\",\"interpretedIndexed\":9}],\"issues\":[{\"issue\":\"TAXON_MATCH_FUZZY\","
            + "\"count\":1}]}]}");

    Metrics metrics = service.get(key).getMetrics();

    assertTrue(metrics.isIndexeable());
    assertEquals(2, metrics.getFileInfos().size());
    Metrics.FileInfo core =
        metrics.getFileInfos().stream()
            .filter(f -> "occurrence.txt".equals(f.getFileName()))
            .findFirst()
            .orElseThrow();
    assertEquals(10L, core.getCount());
    assertEquals(9L, core.getIndexedCount());
    assertEquals(10L, core.getTerms().get(0).getRawIndexed());
    assertEquals(9L, core.getTerms().get(0).getInterpretedIndexed());
    assertEquals(
        List.of("RECORD_NOT_UNIQUELY_IDENTIFIED", "TAXON_MATCH_FUZZY"),
        core.getIssues().stream().map(Metrics.IssueInfo::getIssue).toList());
    assertTrue(metrics.getFileInfos().stream().anyMatch(f -> "eml.xml".equals(f.getFileName())));
  }

  @Test
  void colDpIsIndexeableFromTheChecklistBankEvaluation() {
    UUID key = stored(Status.RUNNING, FileFormat.COLDP);
    UUID run = EngineRows.execution(jdbc, key, "RUNNING", false, null);
    EngineRows.step(jdbc, run, "VALIDATOR_CHECKLISTBANK", 2, "FINISHED", null);
    EngineRows.report(
        jdbc,
        key,
        ReportTypes.CHECKLIST,
        "{\"clbDatasetKey\":4711,\"importStatus\":\"finished\",\"indexeable\":true,"
            + "\"files\":[{\"fileName\":\"NameUsage.tsv\",\"count\":3}]}");

    Metrics metrics = service.get(key).getMetrics();

    assertTrue(metrics.isIndexeable());
    assertEquals("NameUsage.tsv", metrics.getFileInfos().get(0).getFileName());
  }

  @Test
  void withoutReportsTheValidationIsNotIndexeable() {
    UUID key = stored(Status.RUNNING, FileFormat.DWCA);
    EngineRows.execution(jdbc, key, "RUNNING", false, null);

    assertFalse(service.get(key).getMetrics().isIndexeable());
  }

  // --- helpers ---------------------------------------------------------------------------------

  private UUID stored(Status status, FileFormat format) {
    UUID key = UUID.randomUUID();
    validations.create(
        Validation.builder()
            .key(key)
            .username("alice")
            .file("data.zip")
            .status(status)
            .fileFormat(format)
            .build());
    return key;
  }

  private static ChecklistbankRequest request(UUID key) {
    return new ChecklistbankRequest(
        new StepHeader(WORKFLOW, key, 1, StepType.VALIDATOR_CHECKLISTBANK, 1), 4711);
  }

  private PipelinesChecklistValidatorMessage onlyForwarded() {
    assertEquals(1, messages.legacy.size());
    return (PipelinesChecklistValidatorMessage) messages.legacy.get(0);
  }

  private static StepHeader header(PipelinesChecklistValidatorMessage message) {
    return OrchestratorJson.mapper().convertValue(message.getStepHeader(), StepHeader.class);
  }

  private static JsonNode json(String text) throws Exception {
    return OrchestratorJson.mapper().readTree(text);
  }
}
