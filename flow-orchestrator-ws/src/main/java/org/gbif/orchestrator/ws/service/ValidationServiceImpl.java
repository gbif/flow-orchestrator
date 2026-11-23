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

import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.model.registry.Dataset;
import org.gbif.common.messaging.api.messages.PipelinesChecklistValidatorMessage;
import org.gbif.eml.validation.ArchiveMetadata;
import org.gbif.metadata.eml.parse.DatasetEmlParser;
import org.gbif.orchestrator.api.event.WorkflowFinished;
import org.gbif.orchestrator.api.json.OrchestratorJson;
import org.gbif.orchestrator.ws.api.FileFormat;
import org.gbif.orchestrator.ws.api.Validation;
import org.gbif.orchestrator.ws.api.Validation.Status;
import org.gbif.orchestrator.ws.api.ValidationRequest;
import org.gbif.orchestrator.ws.api.ValidationSearchRequest;
import org.gbif.orchestrator.ws.file.ArchiveCoreType;
import org.gbif.orchestrator.ws.file.DataFile;
import org.gbif.orchestrator.ws.file.FileStoreManager;
import org.gbif.orchestrator.ws.file.UnsupportedMediaTypeException;
import org.gbif.orchestrator.ws.mail.ValidatorEmailService;
import org.gbif.orchestrator.ws.messaging.ChecklistbankRequest;
import org.gbif.orchestrator.ws.messaging.Messages;
import org.gbif.orchestrator.ws.persistence.ValidationRepository;
import org.gbif.ws.security.GbifUserPrincipal;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import static org.gbif.orchestrator.ws.service.ValidationFactory.metricsSubmitError;
import static org.gbif.orchestrator.ws.service.ValidationFactory.newValidationInstance;
import static org.gbif.ws.security.UserRoles.ADMIN_ROLE;
import static org.gbif.ws.security.UserRoles.APP_ROLE;

/**
 * Validations: submission (upload or URL), admission, reads, cancel, delete, the legacy worker
 * updates, the ChecklistBank callback and the end of orchestrated workflows.
 *
 * <p>Ported from pipelines-validator-ws with the security fixes of gbif/security-issues#46 (owner,
 * admin or app may modify; no undeleting; the submission task can't overtake the insert). Formats
 * the engine runs ({@link ValidationMessageService#isOrchestrated}) are started with a command;
 * their status and metrics are composed from the workflow when read.
 *
 * <p>An email goes out once, when a validation first reaches FINISHED, FAILED or ABORTED, to its
 * stored notification emails or else its owner.
 */
@Service
public class ValidationServiceImpl {

  private static final Logger LOG = LoggerFactory.getLogger(ValidationServiceImpl.class);
  private static final TypeReference<Map<String, Object>> HEADER_MAP = new TypeReference<>() {};
  private static final String WAIT_HEADER = "header";
  private static final String PENDING_IMPORT = "pendingImport";

  private final FileStoreManager fileStoreManager;
  private final ValidationRepository validations;
  private final Messages messages;
  private final ValidatorEmailService emailService;
  private final ValidationMessageService starter;
  private final WorkflowStatusComposer composer;
  private final ErrorMapper errorMapper;
  private final Clock clock;
  private final int maxRunningValidationPerUser;

  public ValidationServiceImpl(
      FileStoreManager fileStoreManager,
      ValidationRepository validations,
      Messages messages,
      ValidatorEmailService emailService,
      ValidationMessageService starter,
      WorkflowStatusComposer composer,
      ErrorMapper errorMapper,
      Clock clock,
      @Value("${maxRunningValidationPerUser}") int maxRunningValidationPerUser) {
    this.fileStoreManager = fileStoreManager;
    this.validations = validations;
    this.messages = messages;
    this.emailService = emailService;
    this.starter = starter;
    this.composer = composer;
    this.errorMapper = errorMapper;
    this.clock = clock;
    this.maxRunningValidationPerUser = maxRunningValidationPerUser;
  }

  // ---------------------------------------------------------------------------------------------
  // Submission
  // ---------------------------------------------------------------------------------------------

  /** Asserts the user has not reached the maximum number of executing validations. */
  public boolean reachedMaxRunningValidations(String userName) {
    boolean reachedMaximum =
        validations.count(
                userName,
                ValidationSearchRequest.builder().status(Validation.executingStatuses()).build())
            >= maxRunningValidationPerUser;
    if (reachedMaximum) {
      LOG.info("User {} reached maximum running validations", userName);
    }
    return reachedMaximum;
  }

  /** Admission: notification emails for installations, and the per-user running cap. */
  public void validateRequest(ValidationRequest validationRequest) {
    if (validationRequest.getInstallationKey() != null
        && (validationRequest.getNotificationEmail() == null
            || validationRequest.getNotificationEmail().isEmpty())) {
      throw errorMapper.apply(Validation.ErrorCode.NOTIFICATION_EMAILS_MISSING);
    }
    if (reachedMaxRunningValidations(getPrincipal().getUsername())) {
      throw errorMapper.apply(Validation.ErrorCode.MAX_RUNNING_VALIDATIONS);
    }
  }

  public Validation validateFile(MultipartFile file, ValidationRequest validationRequest) {
    validateRequest(validationRequest);

    LOG.info("Starting validation for the file {}", file.getOriginalFilename());
    UUID key = UUID.randomUUID();
    FileStoreManager.AsyncDataFileTask task = fileStoreManager.uploadDataFile(file, key.toString());
    Validation validation = create(key, task.getStart(), Status.SUBMITTED, validationRequest);
    // Attached once the validation is persisted, so its update can't run before the insert
    task.getTask()
        .whenCompleteAsync(
            (df, tr) -> {
              if (tr == null) {
                LOG.info("File has been uploaded and decompressed, key {}", key);
                updateAndNotifySubmitted(key, df);
              } else {
                Throwable cause = unwrap(tr);
                LOG.error("Error submitting file of validation {}", key, cause);
                String message = "Error during the file submitting. ";
                if (cause instanceof UnsupportedMediaTypeException) {
                  message = "";
                } else if (cause.getClass().getSimpleName().equals("UnsupportedCompressionType")) {
                  message += "Please check the archive compression type. ";
                }
                updateFailedValidation(key, message + cause.getMessage());
              }
            });
    return validation;
  }

  public Validation validateFileFromUrl(String fileURL, ValidationRequest validationRequest) {
    try {
      validateRequest(validationRequest);

      LOG.info("Starting validation for the URL {}", fileURL);
      UUID key = UUID.randomUUID();
      String encodedFileURL = EncodingUtil.encode(fileURL);

      FileStoreManager.AsyncDownloadResult downloadResult =
          fileStoreManager.downloadDataFile(encodedFileURL, key.toString());
      Validation validation =
          create(key, downloadResult.getDataFile(), Status.DOWNLOADING, validationRequest);
      // Attached once the validation is persisted, so its update can't run before the insert
      downloadResult
          .getDownloadTask()
          .whenCompleteAsync(
              (resultDataFile, err) -> {
                if (err != null) {
                  Throwable cause = unwrap(err);
                  LOG.error("Error processing file of validation {}", key, cause);
                  updateFailedValidation(key, cause.getMessage());
                } else if (resultDataFile == null) {
                  LOG.error("Downloaded file is null for URL {}, key {}", fileURL, key);
                  updateFailedValidation(key, "Downloaded file from " + fileURL + " is null");
                } else {
                  LOG.info(
                      "File has been downloaded and decompressed from URL {}, key {} to path {}",
                      fileURL,
                      key,
                      resultDataFile.getFilePath().toAbsolutePath());
                  updateAndNotifySubmitted(key, resultDataFile);
                }
              });
      return validation;
    } catch (IOException ex) {
      LOG.error("Can't download submitted file", ex);
      throw errorMapper.apply(Validation.ErrorCode.IO_ERROR);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Reads
  // ---------------------------------------------------------------------------------------------

  /**
   * Gets a validation by its key; any user may read any validation (gbif/portal-feedback#3844).
   * Orchestrated validations get their status and metrics from the workflow.
   */
  public Validation get(UUID key) {
    return composer.compose(stored(key));
  }

  public Dataset getDataset(UUID key) {
    return stored(key).getDataset();
  }

  /** Paged result of the current user's validations. */
  public PagingResponse<Validation> list(ValidationSearchRequest request) {
    String username = getPrincipal().getUsername();
    long total = validations.count(username, request);
    List<Validation> page =
        validations.list(username, request).stream().map(composer::compose).toList();
    return new PagingResponse<>(request.getOffset(), request.getLimit(), total, page);
  }

  public List<UUID> getRunningValidations(int min) {
    return validations.getRunningValidations(
        Date.from(clock.instant().minus(Duration.ofMinutes(min))));
  }

  // ---------------------------------------------------------------------------------------------
  // Changes
  // ---------------------------------------------------------------------------------------------

  /**
   * The legacy worker update (GET, modify, PUT). Refused for orchestrated validations: the engine
   * owns their state.
   */
  public Validation update(Validation validation) {
    if (validation.getKey() == null) {
      throw errorMapper.apply(Validation.ErrorCode.NOT_FOUND);
    }
    Validation current = stored(validation.getKey());
    checkCanModify(current);
    if (ValidationMessageService.isOrchestrated(current.getFileFormat())) {
      throw errorMapper.apply(Validation.ErrorCode.ORCHESTRATED);
    }
    LOG.info("Updating validation key {}, status {}", validation.getKey(), validation.getStatus());
    validations.update(validation);
    notifyIfFinished(current, validation.getStatus());
    return get(validation.getKey());
  }

  /** Cancels an executing validation; an orchestrated one also gets its workflow cancelled. */
  public Validation cancel(UUID key) {
    Validation validation = stored(key);
    checkCanModify(validation);
    if (!validation.isExecuting()) {
      throw errorMapper.apply(Validation.ErrorCode.VALIDATION_IS_NOT_EXECUTING);
    }
    LOG.info("Cancel validation record for key {}", key);
    Validation cancelled =
        Validation.builder().key(key).status(Status.ABORTED).metrics(validation.getMetrics()).build();
    validations.update(cancelled);
    if (ValidationMessageService.isOrchestrated(validation.getFileFormat())) {
      try {
        starter.cancel(key, getPrincipal().getUsername());
      } catch (IOException e) {
        // The validation is cancelled for the user; the workflow ends when its steps reply.
        LOG.error("Could not send CANCEL for validation {}", key, e);
      }
    }
    fileStoreManager.deleteIfExist(key.toString());
    notifyIfFinished(validation, Status.ABORTED);
    return get(key);
  }

  public void delete(UUID key) {
    Validation validation = stored(key);
    checkCanModify(validation);
    LOG.info("Delete validation record for key {}", key);
    validations.delete(validation.getKey());
    fileStoreManager.deleteIfExist(key.toString());
  }

  // ---------------------------------------------------------------------------------------------
  // ChecklistBank
  // ---------------------------------------------------------------------------------------------

  /**
   * The engine's VALIDATOR_CHECKLISTBANK request: the archive validator submitted the checklist to
   * ChecklistBank. Records the wait (the submitted dataset and the step's header) so the
   * ChecklistBank callback, or the poller, can hand the import to the checklist validator. If
   * ChecklistBank already called back (it was faster than this request), the import it sent is
   * handed over now. A validation that was cancelled or deleted meanwhile is left alone.
   */
  public void awaitChecklistbank(ChecklistbankRequest request) {
    UUID key = request.header().datasetKey();
    Optional<Validation> found = validations.get(key);
    if (found.isEmpty()) {
      LOG.warn("ChecklistBank step for unknown validation {}", key);
      return;
    }
    Validation validation = found.get();
    Optional<JsonNode> early = pendingImport(validation);
    validation.setClbDatasetKey(Math.toIntExact(request.clbDatasetKey()));
    validation.setClbValidationMessage(toJson(request));
    validation.setStatus(Status.WAITING_FOR_CHECKLISTBANK);
    if (validations.updateIfExecuting(validation) == 0) {
      LOG.info("Validation {} is not executing anymore, not waiting for ChecklistBank", key);
      return;
    }
    LOG.info("Validation {} waits for ChecklistBank dataset {}", key, request.clbDatasetKey());
    if (early.isPresent()) {
      try {
        forwardChecklistImport(validation, request, text(early.get()));
      } catch (Exception e) {
        // Still waiting: the poller hands the import over later.
        LOG.error("Could not hand the early ChecklistBank import of {} over", key, e);
      }
    }
  }

  /** The orchestrated ChecklistBank wait recorded for the validation, if any. */
  public Optional<ChecklistbankRequest> checklistbankWait(Validation validation) {
    return storedClb(validation)
        .filter(node -> node.path(WAIT_HEADER).isObject())
        .flatMap(
            node -> {
              try {
                return Optional.of(
                    OrchestratorJson.mapper().treeToValue(node, ChecklistbankRequest.class));
              } catch (Exception e) {
                LOG.warn("Unreadable ChecklistBank wait of validation {}", validation.getKey(), e);
                return Optional.empty();
              }
            });
  }

  /** An import ChecklistBank sent before the wait was recorded (JSON null if it sent none). */
  private Optional<JsonNode> pendingImport(Validation validation) {
    return storedClb(validation)
        .filter(node -> node.has(PENDING_IMPORT))
        .map(node -> node.get(PENDING_IMPORT));
  }

  /**
   * {@code clb_validation_message} as JSON: the recorded wait ({@code {"header", "clbDatasetKey"}}),
   * an early import ({@code {"pendingImport"}}), or for legacy validations the archive validator's
   * message.
   */
  private Optional<JsonNode> storedClb(Validation validation) {
    String stored = validation.getClbValidationMessage();
    if (stored == null || stored.isBlank()) {
      return Optional.empty();
    }
    try {
      return Optional.of(OrchestratorJson.mapper().readTree(stored));
    } catch (Exception e) {
      LOG.warn("Unreadable ChecklistBank state of validation {}", validation.getKey(), e);
      return Optional.empty();
    }
  }

  private static String toJson(Object value) {
    try {
      return OrchestratorJson.mapper().writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Cannot write " + value, e);
    }
  }

  private static String text(JsonNode clbDatasetImport) {
    return clbDatasetImport == null || clbDatasetImport.isNull()
        ? null
        : clbDatasetImport.toString();
  }

  /**
   * Hands a ChecklistBank import (as ChecklistBank sent it, or null) to the checklist validator,
   * with the waiting step's header: the validator evaluates it and replies to the engine. The
   * validation stops waiting, so the poller doesn't hand it over again.
   */
  public void forwardChecklistImport(
      Validation validation, ChecklistbankRequest wait, String clbDatasetImport)
      throws IOException {
    messages.send(
        new PipelinesChecklistValidatorMessage(validation.getKey(), clbDatasetImport)
            .setStepHeader(OrchestratorJson.mapper().convertValue(wait.header(), HEADER_MAP)));
    validation.setStatus(Status.RUNNING);
    validations.updateIfExecuting(validation);
    LOG.info(
        "ChecklistBank import of validation {} handed to the checklist validator",
        validation.getKey());
  }

  /** ChecklistBank's callback: hands the import to the checklist validator. */
  public void validateChecklistResults(UUID validationKey, JsonNode clbDatasetImport) {
    Validation validation = stored(validationKey);

    if (!validation.isExecuting()) {
      throw errorMapper.apply(Validation.ErrorCode.VALIDATION_IS_NOT_EXECUTING);
    }

    Optional<ChecklistbankRequest> wait = checklistbankWait(validation);
    if (wait.isPresent()) {
      try {
        forwardChecklistImport(validation, wait.get(), text(clbDatasetImport));
      } catch (Exception e) {
        // Still waiting: the poller hands the import over later.
        LOG.error("Could not hand the ChecklistBank import of {} over", validationKey, e);
      }
      return;
    }

    if (ValidationMessageService.isOrchestrated(validation.getFileFormat())
        && storedClb(validation).isEmpty()) {
      // ChecklistBank was faster than the engine's ChecklistBank step: keep the import until the
      // wait is recorded (awaitChecklistbank hands it over then).
      ObjectNode pending = OrchestratorJson.mapper().createObjectNode();
      pending.set(
          PENDING_IMPORT, clbDatasetImport == null ? NullNode.getInstance() : clbDatasetImport);
      validation.setClbValidationMessage(pending.toString());
      validations.updateIfExecuting(validation);
      LOG.info("ChecklistBank import of {} arrived before its wait, kept", validationKey);
      return;
    }

    // Legacy validations (started before Round B)
    if (clbDatasetImport == null || clbDatasetImport.isNull()) {
      LOG.info("CLB validation response for {} is null", validationKey);
      updateChecklistValidatorStatus(validation, Status.FAILED);
      return;
    }

    try {
      messages.send(
          new PipelinesChecklistValidatorMessage(validationKey, clbDatasetImport.toString()));
    } catch (Exception e) {
      LOG.error("Error processing CLB validation response for {}", validationKey, e);
      updateChecklistValidatorStatus(validation, Status.FAILED);
    }
  }

  /**
   * Sets the archive step and the validation status (ChecklistBank callback and poller), for
   * legacy validations only; orchestrated ones end with their workflow.
   */
  public void updateChecklistValidatorStatus(Validation validation, Status newStatus) {
    Status previous = validation.getStatus();
    validation.getMetrics().getStepTypes().stream()
        .filter(step -> step.getStepType().equals(StepType.VALIDATOR_VALIDATE_ARCHIVE.name()))
        .forEach(step -> step.setStatus(newStatus));
    validation.setStatus(newStatus);
    validations.update(validation);
    notifyIfFinished(Validation.builder().key(validation.getKey()).status(previous).build(), newStatus);
  }

  /**
   * The engine ended an orchestrated validation's workflow: store the final status and the
   * composed metrics, and email. Repeats, and events for validations already cancelled or
   * deleted, change nothing.
   */
  public void workflowFinished(WorkflowFinished event) {
    Optional<Validation> found = validations.get(event.datasetKey());
    if (found.isEmpty()) {
      LOG.warn("WorkflowFinished for unknown validation {}", event.datasetKey());
      return;
    }
    Status status = WorkflowStatusComposer.finalStatus(event.result().name());
    Validation composed = composer.compose(found.get());
    if (validations.finish(event.datasetKey(), status, composed.getMetrics())) {
      LOG.info(
          "Validation {} finished as {} (rejected={}, reason={})",
          event.datasetKey(),
          status,
          event.rejected(),
          event.reason());
      email(stored(event.datasetKey()));
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Internals
  // ---------------------------------------------------------------------------------------------

  private Validation stored(UUID key) {
    return validations
        .get(key)
        .orElseThrow(() -> errorMapper.apply(Validation.ErrorCode.NOT_FOUND));
  }

  private GbifUserPrincipal getPrincipal() {
    Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
    if (authentication != null
        && authentication.isAuthenticated()
        && authentication.getPrincipal() instanceof GbifUserPrincipal principal) {
      return principal;
    }
    throw new SecurityException("User credentials not found");
  }

  /** Asserts the current user created the validation, or is an admin or an app. */
  private void checkCanModify(Validation validation) {
    GbifUserPrincipal principal = getPrincipal();
    boolean adminOrApp =
        principal.getAuthorities().stream()
            .map(GrantedAuthority::getAuthority)
            .anyMatch(role -> ADMIN_ROLE.equals(role) || APP_ROLE.equals(role));
    if (!adminOrApp && !principal.getUsername().equals(validation.getUsername())) {
      LOG.warn(
          "User {} is not allowed to modify validation {}",
          principal.getUsername(),
          validation.getKey());
      throw errorMapper.apply(Validation.ErrorCode.AUTHORIZATION_ERROR);
    }
  }

  private Validation create(
      UUID key, DataFile dataFile, Status status, ValidationRequest validationRequest) {
    validations.create(
        newValidationInstance(
            key,
            dataFile,
            getPrincipal().getUsername(), // this will validate credentials
            status,
            validationRequest.getSourceId(),
            validationRequest.getInstallationKey(),
            validationRequest.getNotificationEmail()));
    LOG.info("Create validation record for key {}", key);
    return stored(key);
  }

  /** The file is in place: record what it is and start the work. */
  private void updateAndNotifySubmitted(UUID key, DataFile dataFile) {
    try {
      Validation v = validations.get(key).orElse(newValidationInstance(key, dataFile, Status.QUEUED));
      Path folder = dataFile.getFilePath().getParent();
      boolean checklist =
          dataFile.getFileFormat() == FileFormat.DWCA && ArchiveCoreType.isChecklist(folder);

      v.setStatus(Status.QUEUED);
      v.setFileFormat(dataFile.getFileFormat());
      v.setFileSize(Files.size(dataFile.getFilePath()));
      v.setFile(dataFile.getSourceFileName());
      // The workflow's steps follow once the engine has started it (composed when read).
      v.getMetrics().setStepTypes(StepsMapper.getUploadingSteps(Status.FINISHED, null));
      v.setDataset(readEml(folder));

      // Unless the validation was cancelled or deleted meanwhile
      if (validations.updateIfExecuting(v) == 0) {
        LOG.info("Validation {} is not executing anymore, not started", key);
        return;
      }
      starter.start(key, dataFile.getFileFormat(), checklist, v.getUsername());
    } catch (Exception ex) {
      LOG.error("Failed to submit validation {}", key, ex);
      updateFailedValidation(key, ex.getMessage());
    }
  }

  private Dataset readEml(Path pathToArchive) {
    try {
      Optional<Path> existedPath = ArchiveMetadata.find(pathToArchive);
      if (existedPath.isEmpty() || !Files.exists(existedPath.get())) {
        LOG.info("No metadata document in {}", pathToArchive);
        return null;
      }
      byte[] eml = Files.readAllBytes(existedPath.get());
      // Set the context classloader to ensure Digester can find GBIF classes
      Thread currentThread = Thread.currentThread();
      ClassLoader originalClassLoader = currentThread.getContextClassLoader();
      try {
        currentThread.setContextClassLoader(getClass().getClassLoader());
        return DatasetEmlParser.build(eml);
      } finally {
        currentThread.setContextClassLoader(originalClassLoader);
      }
    } catch (Exception ex) {
      LOG.error("Can't parse eml file and convert to Dataset. {}", ex.getMessage());
      return null;
    }
  }

  /** Marks a validation that could not be submitted as FAILED and emails its owner. */
  private void updateFailedValidation(UUID key, String errorMessage) {
    LOG.error("Failed validation for key {} with error {}", key, errorMessage);
    Validation failed = newValidationInstance(key, Status.FAILED, metricsSubmitError(errorMessage));
    if (validations.updateIfExecuting(failed) == 1) {
      email(stored(key));
    } else {
      LOG.info("Validation {} is not executing anymore, not updated", key);
    }
  }

  /** Emails when this change is the validation's first move to a final status. */
  private void notifyIfFinished(Validation before, Status after) {
    if (!before.hasFinished() && Validation.finishedStatuses().contains(after)) {
      LOG.info("Validation {} finished with status {}", before.getKey(), after);
      email(stored(before.getKey()));
    }
  }

  /**
   * Sends the validation's email. Best effort: the status is already stored, so a mail or user
   * lookup failure is logged and must not fail the request or the WorkflowFinished handling.
   */
  private void email(Validation validation) {
    try {
      emailService.sendEmailNotification(validation);
    } catch (RuntimeException e) {
      LOG.error("Could not email about validation {}", validation.getKey(), e);
    }
  }

  private static Throwable unwrap(Throwable t) {
    return t instanceof CompletionException && t.getCause() != null ? t.getCause() : t;
  }

  /** Visible for the jobs: the stored row, without composition. */
  public Optional<Validation> find(UUID key) {
    return validations.get(key);
  }
}
