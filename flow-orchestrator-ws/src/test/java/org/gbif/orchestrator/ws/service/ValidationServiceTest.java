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
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.orchestrator.api.command.CommandType;
import org.gbif.orchestrator.api.command.Domain;
import org.gbif.orchestrator.api.command.WorkflowCommand;
import org.gbif.orchestrator.api.event.WorkflowFinished;
import org.gbif.orchestrator.api.event.WorkflowResult;
import org.gbif.orchestrator.api.report.ReportTypes;
import org.gbif.orchestrator.ws.api.FileFormat;
import org.gbif.orchestrator.ws.api.Metrics.ValidationStep;
import org.gbif.orchestrator.ws.api.Validation;
import org.gbif.orchestrator.ws.api.Validation.Status;
import org.gbif.orchestrator.ws.api.ValidationRequest;
import org.gbif.orchestrator.ws.persistence.ValidationRepository;
import org.gbif.orchestrator.ws.support.Archives;
import org.gbif.orchestrator.ws.support.EngineRows;
import org.gbif.orchestrator.ws.support.FakeMessages;
import org.gbif.orchestrator.ws.support.RecordingEmails;
import org.gbif.orchestrator.ws.support.TestBeans;
import org.gbif.orchestrator.ws.support.Users;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.web.server.ResponseStatusException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@Import(TestBeans.class)
class ValidationServiceTest {

  private static final ValidationRequest NO_REQUEST = ValidationRequest.builder().build();

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
    emails.fail = false;
    Users.login("alice", UserRole.USER);
  }

  @AfterEach
  void tearDown() {
    Users.logout();
  }

  // --- submission ------------------------------------------------------------------------------

  @Test
  void dwcDpUploadStartsAWorkflow() throws Exception {
    Validation submitted = service.validateFile(Archives.dwcDp("pumas.zip"), NO_REQUEST);
    assertEquals(Status.SUBMITTED, submitted.getStatus());

    Validation queued = await(submitted.getKey(), v -> v.getStatus() == Status.QUEUED);
    assertEquals(FileFormat.DWCDP, queued.getFileFormat());
    assertEquals("pumas.zip", queued.getFile());

    assertEquals(1, messages.commands.size());
    WorkflowCommand start = messages.commands.get(0);
    assertEquals(CommandType.START, start.type());
    assertEquals(Domain.VALIDATION, start.domain());
    assertEquals(submitted.getKey(), start.datasetKey());
    assertEquals(1, start.attempt());
    assertEquals(Map.of("fileFormat", "DWCDP"), start.parameters());
    assertEquals("alice", start.requestedBy());
    assertTrue(messages.legacy.isEmpty());
  }

  @Test
  void dwcaUploadKeepsTheLegacyStart() throws Exception {
    Validation submitted = service.validateFile(Archives.dwca("cats.zip"), NO_REQUEST);

    Validation queued = await(submitted.getKey(), v -> v.getStatus() == Status.QUEUED);
    assertEquals(FileFormat.DWCA, queued.getFileFormat());
    assertEquals("Cats of the example valley", queued.getDataset().getTitle());

    assertTrue(messages.commands.isEmpty());
    PipelinesArchiveValidatorMessage start =
        (PipelinesArchiveValidatorMessage) messages.legacy.get(0);
    assertEquals(submitted.getKey(), start.getDatasetUuid());
    assertEquals("DWCA", start.getFileFormat());
    assertTrue(start.getPipelineSteps().contains(StepType.VALIDATOR_VALIDATE_ARCHIVE.name()));
  }

  @Test
  void uploadNamesCannotEscapeTheStore() throws Exception {
    Validation submitted = service.validateFile(Archives.dwcDp("../../evil.zip"), NO_REQUEST);
    assertEquals("evil.zip", await(submitted.getKey(), v -> v.getFile() != null).getFile());
  }

  @Test
  void aFailedStartFailsTheValidationAndEmailsTheOwner() throws Exception {
    messages.fail = true;
    Validation submitted = service.validateFile(Archives.dwcDp("pumas.zip"), NO_REQUEST);

    Validation failed = await(submitted.getKey(), v -> v.getStatus() == Status.FAILED);
    assertTrue(failed.getMetrics().getStepTypes().get(0).getMessage().contains("broker down"));
    awaitEmails(1);
    assertEquals("alice", emails.sent.get(0).getUsername());
  }

  @Test
  void installationRequestsNeedNotificationEmails() {
    ValidationRequest request = ValidationRequest.builder().installationKey(UUID.randomUUID()).build();
    assertStatus(HttpStatus.BAD_REQUEST, () -> service.validateRequest(request));
  }

  @Test
  void theRunningCapIsPerUser() {
    for (int i = 0; i < 3; i++) {
      stored("alice", Status.RUNNING, FileFormat.DWCA);
    }
    assertStatus(HttpStatus.valueOf(420), () -> service.validateRequest(NO_REQUEST));

    Users.login("bob", UserRole.USER);
    service.validateRequest(NO_REQUEST);
  }

  // --- reads -----------------------------------------------------------------------------------

  @Test
  void orchestratedStatusAndMetricsComeFromTheWorkflow() {
    UUID key = stored("alice", Status.QUEUED, FileFormat.DWCDP);
    UUID run = EngineRows.execution(jdbc, key, "RUNNING", false, null);
    EngineRows.step(jdbc, run, "VALIDATOR_VALIDATE_DWCDP", 0, "FINISHED", null);
    EngineRows.step(jdbc, run, "VALIDATOR_DWCDP_STAGE", 1, "RUNNING", null);
    EngineRows.step(jdbc, run, "VALIDATOR_DWCDP_TO_VERBATIM", 2, "PENDING", null);

    Validation read = service.get(key);

    assertEquals(Status.RUNNING, read.getStatus());
    List<ValidationStep> steps = read.getMetrics().getStepTypes();
    assertEquals(
        List.of(
            "VALIDATOR_UPLOAD_ARCHIVE",
            "VALIDATOR_VALIDATE_DWCDP",
            "VALIDATOR_DWCDP_STAGE",
            "VALIDATOR_DWCDP_TO_VERBATIM"),
        steps.stream().map(ValidationStep::getStepType).toList());
    assertEquals(
        java.util.Arrays.asList(Status.FINISHED, Status.FINISHED, Status.RUNNING, null),
        steps.stream().map(ValidationStep::getStatus).toList());
    assertEquals(List.of(0, 1, 2, 3), steps.stream().map(ValidationStep::getExecutionOrder).toList());
  }

  @Test
  void interpretationMetricsBecomeTheFilesOfTheValidation() {
    UUID key = stored("alice", Status.QUEUED, FileFormat.DWCDP);
    UUID run = EngineRows.execution(jdbc, key, "RUNNING", false, null);
    EngineRows.step(jdbc, run, "VALIDATOR_COLLECT_METRICS", 0, "FINISHED", null);
    EngineRows.report(
        jdbc,
        key,
        ReportTypes.INTERPRETATION_METRICS,
        "{\"indexeable\":true,\"files\":[{\"fileType\":\"CORE\",\"indexedCount\":42,"
            + "\"rowType\":\"http://rs.tdwg.org/dwc/terms/Occurrence\"}],\"stepTypes\":[]}");

    Validation read = service.get(key);

    assertTrue(read.getMetrics().isIndexeable());
    assertEquals(42L, read.getMetrics().getFileInfos().get(0).getIndexedCount());
  }

  @Test
  void legacyValidationsKeepTheirStoredMetrics() {
    UUID key = stored("alice", Status.RUNNING, FileFormat.DWCA);
    assertEquals(Status.RUNNING, service.get(key).getStatus());
    assertEquals("stored", service.get(key).getMetrics().getError());
  }

  @Test
  void anUnknownKeyIs404() {
    assertStatus(HttpStatus.NOT_FOUND, () -> service.get(UUID.randomUUID()));
  }

  // --- end of an orchestrated workflow --------------------------------------------------------

  @Test
  void workflowFinishedStoresTheResultAndEmailsOnce() {
    UUID key = stored("alice", Status.QUEUED, FileFormat.DWCDP);
    UUID run = EngineRows.execution(jdbc, key, "FINISHED", false, null);
    EngineRows.step(jdbc, run, "VALIDATOR_VALIDATE_DWCDP", 0, "FINISHED", null);

    service.workflowFinished(finished(run, key, WorkflowResult.FINISHED, false, null));
    service.workflowFinished(finished(run, key, WorkflowResult.FINISHED, false, null));

    Validation stored = validations.get(key).orElseThrow();
    assertEquals(Status.FINISHED, stored.getStatus());
    assertEquals("VALIDATOR_VALIDATE_DWCDP", stored.getMetrics().getStepTypes().get(1).getStepType());
    assertEquals(1, emails.sent.size());
    assertEquals(Status.FINISHED, emails.sent.get(0).getStatus());
  }

  @Test
  void aMailFailureDoesNotLoseTheResult() {
    UUID key = stored("alice", Status.QUEUED, FileFormat.DWCDP);
    UUID run = EngineRows.execution(jdbc, key, "FINISHED", false, null);
    emails.fail = true;

    service.workflowFinished(finished(run, key, WorkflowResult.FINISHED, false, null));

    assertEquals(Status.FINISHED, validations.get(key).orElseThrow().getStatus());
  }

  @Test
  void aRejectionIsAFinishedValidationThatCannotBeIndexed() {
    UUID key = stored("alice", Status.QUEUED, FileFormat.DWCDP);
    UUID run = EngineRows.execution(jdbc, key, "FINISHED", true, "The data package is not valid");
    EngineRows.step(jdbc, run, "VALIDATOR_VALIDATE_DWCDP", 0, "FINISHED", null);
    EngineRows.step(jdbc, run, "VALIDATOR_DWCDP_STAGE", 1, "SKIPPED", "Invalid data package");

    service.workflowFinished(
        finished(run, key, WorkflowResult.FINISHED, true, "The data package is not valid"));

    Validation read = service.get(key);
    assertEquals(Status.FINISHED, read.getStatus());
    assertFalse(read.getMetrics().isIndexeable());
    assertEquals("The data package is not valid", read.getMetrics().getError());
    ValidationStep skipped = read.getMetrics().getStepTypes().get(2);
    assertEquals(Status.ABORTED, skipped.getStatus());
    assertEquals("Skipped: Invalid data package", skipped.getMessage());
  }

  @Test
  void notificationEmailsAreKept() {
    UUID key = stored("alice", Status.QUEUED, FileFormat.DWCDP);
    jdbc.sql("UPDATE validation SET notification_emails = '[\"ipt@example.org\"]' WHERE key = :k")
        .param("k", key)
        .update();
    UUID run = EngineRows.execution(jdbc, key, "FAILED", false, "Archive not found");

    service.workflowFinished(finished(run, key, WorkflowResult.FAILED, false, "Archive not found"));

    assertEquals(Set.of("ipt@example.org"), emails.sent.get(0).getNotificationEmails());
    assertEquals(Status.FAILED, emails.sent.get(0).getStatus());
  }

  // --- changes ---------------------------------------------------------------------------------

  @Test
  void cancellingAnOrchestratedValidationCancelsItsWorkflow() {
    UUID key = stored("alice", Status.QUEUED, FileFormat.DWCDP);

    Validation cancelled = service.cancel(key);

    assertEquals(Status.ABORTED, cancelled.getStatus());
    assertEquals(CommandType.CANCEL, messages.commands.get(0).type());
    assertEquals(key, messages.commands.get(0).datasetKey());
    assertEquals(1, emails.sent.size());

    // the engine's WorkflowFinished (ABORTED) afterwards changes nothing and sends no email
    service.workflowFinished(finished(UUID.randomUUID(), key, WorkflowResult.ABORTED, false, null));
    assertEquals(1, emails.sent.size());
  }

  @Test
  void onlyTheOwnerOrAnAdminMayModify() {
    UUID key = stored("alice", Status.QUEUED, FileFormat.DWCDP);

    Users.login("mallory", UserRole.USER);
    assertStatus(HttpStatus.FORBIDDEN, () -> service.cancel(key));
    assertStatus(HttpStatus.FORBIDDEN, () -> service.delete(key));

    Users.login("admin", UserRole.REGISTRY_ADMIN);
    service.delete(key);
    assertTrue(validations.get(key).orElseThrow().getDeleted() != null);
  }

  @Test
  void legacyUpdatesAreRefusedForOrchestratedValidations() {
    UUID key = stored("alice", Status.QUEUED, FileFormat.DWCDP);
    Validation update = Validation.builder().key(key).status(Status.FINISHED).build();
    assertStatus(HttpStatus.CONFLICT, () -> service.update(update));
  }

  @Test
  void aLegacyUpdateEmailsOnTheFirstFinalStatusOnly() {
    UUID key = stored("alice", Status.RUNNING, FileFormat.DWCA);
    Validation done = Validation.builder().key(key).status(Status.FINISHED).build();

    service.update(done);
    service.update(done);

    assertEquals(1, emails.sent.size());
  }

  @Test
  void aDeletedValidationStaysDeleted() {
    UUID key = stored("alice", Status.RUNNING, FileFormat.DWCA);
    service.delete(key);

    service.update(Validation.builder().key(key).status(Status.FINISHED).build());

    assertTrue(validations.get(key).orElseThrow().getDeleted() != null);
  }

  // --- helpers ---------------------------------------------------------------------------------

  private UUID stored(String user, Status status, FileFormat format) {
    UUID key = UUID.randomUUID();
    Validation v =
        Validation.builder()
            .key(key)
            .username(user)
            .file("data.zip")
            .status(status)
            .fileFormat(format)
            .build();
    v.getMetrics().setError("stored");
    validations.create(v);
    return key;
  }

  private static WorkflowFinished finished(
      UUID run, UUID key, WorkflowResult result, boolean rejected, String reason) {
    return new WorkflowFinished(
        run, Domain.VALIDATION, key, 1, result, rejected, reason, List.of(), Instant.now());
  }

  private Validation await(UUID key, Predicate<Validation> condition) throws InterruptedException {
    // Generous: the first EML parse in a JVM (gbif-metadata, gbif-common parsers) can take >10s.
    Instant deadline = Instant.now().plus(Duration.ofSeconds(30));
    while (Instant.now().isBefore(deadline)) {
      Validation v = validations.get(key).orElseThrow();
      if (condition.test(v)) {
        return v;
      }
      Thread.sleep(50);
    }
    throw new AssertionError("Validation " + key + " never matched: " + validations.get(key));
  }

  private void awaitEmails(int count) throws InterruptedException {
    Instant deadline = Instant.now().plus(Duration.ofSeconds(10));
    while (emails.sent.size() < count && Instant.now().isBefore(deadline)) {
      Thread.sleep(50);
    }
    assertEquals(count, emails.sent.size());
  }

  private static void assertStatus(HttpStatus expected, Runnable call) {
    ResponseStatusException e = assertThrows(ResponseStatusException.class, call::run);
    assertEquals(expected, e.getStatusCode());
  }
}
