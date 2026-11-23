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
import org.gbif.orchestrator.api.report.ReportTypes;
import org.gbif.orchestrator.ws.api.Metrics;
import org.gbif.orchestrator.ws.api.Metrics.ValidationStep;
import org.gbif.orchestrator.ws.api.Validation;
import org.gbif.orchestrator.ws.api.Validation.Status;
import org.gbif.orchestrator.ws.persistence.WorkflowReader;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Builds the {@code status} and {@code metrics} of an orchestrated validation from its workflow,
 * in the shape validator-ws stored them, so the data-validator UI reads them unchanged:
 *
 * <ul>
 *   <li>{@code stepTypes}: VALIDATOR_UPLOAD_ARCHIVE (finished, order 0), then the workflow's steps
 *       in order (1, 2, …), with status and reason. Not-yet-started steps have no status, as
 *       before. The ChecklistBank wait is shown inside VALIDATOR_VALIDATE_ARCHIVE, where
 *       validator-ws kept it: that step is WAITING_FOR_CHECKLISTBANK until the import is evaluated.
 *   <li>{@code files}: the {@code ARCHIVE_VALIDATION} and {@code CHECKLIST} file infos merged by
 *       file name, then the {@code INTERPRETATION_METRICS} merged in by row type, as the legacy
 *       workers merged them ({@link MetricsMerge}).
 *   <li>{@code indexeable}: from the interpretation metrics ({@code indexedCount > 0}), or for
 *       ColDP from the ChecklistBank evaluation. (validator-ws set it true once metrics ran.)
 *   <li>{@code error}: the workflow's reason when it failed or rejected the data.
 *   <li>{@code status} while running: WAITING_FOR_CHECKLISTBANK during the wait, RUNNING once a
 *       step runs, else QUEUED.
 * </ul>
 */
@Component
public class WorkflowStatusComposer {

  private final WorkflowReader workflows;
  private final ObjectMapper json =
      new ObjectMapper()
          .findAndRegisterModules()
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  public WorkflowStatusComposer(WorkflowReader workflows) {
    this.workflows = workflows;
  }

  /** The validation with composed status and metrics, if it has a workflow; else unchanged. */
  public Validation compose(Validation validation) {
    Optional<WorkflowReader.Execution> execution = workflows.latest(validation.getKey());
    if (execution.isEmpty()) {
      return validation;
    }
    WorkflowReader.Execution run = execution.get();
    List<WorkflowReader.Step> steps = workflows.steps(run.id());

    Metrics metrics = metrics(validation.getKey());
    metrics.setStepTypes(stepTypes(steps));
    if (run.rejected() || "FAILED".equals(run.status())) {
      metrics.setError(run.reason());
      if (run.rejected()) {
        metrics.setIndexeable(false);
      }
    }
    validation.setMetrics(metrics);
    // A cancel or a delete is the row's own state; otherwise the workflow decides.
    if (!validation.hasFinished()) {
      validation.setStatus(status(run.status(), steps));
    }
    return validation;
  }

  /**
   * The validation status for a finished workflow: FINISHED (also when the data was rejected),
   * FAILED or ABORTED.
   */
  static Status finalStatus(String workflowResult) {
    return switch (workflowResult) {
      case "FINISHED" -> Status.FINISHED;
      case "ABORTED" -> Status.ABORTED;
      default -> Status.FAILED;
    };
  }

  /** The steps' reports as the validation's metrics (files and indexeable). */
  private Metrics metrics(UUID key) {
    Optional<Metrics> archive = report(key, ReportTypes.ARCHIVE_VALIDATION);
    Optional<Metrics> checklist = report(key, ReportTypes.CHECKLIST);
    Optional<Metrics> interpretation = report(key, ReportTypes.INTERPRETATION_METRICS);

    List<Metrics.FileInfo> files = new ArrayList<>();
    for (Optional<Metrics> partial : List.of(archive, checklist)) {
      for (Metrics.FileInfo file : partial.map(Metrics::getFileInfos).orElse(List.of())) {
        files = MetricsMerge.mergeByFileName(files, file);
      }
    }
    boolean indexeable;
    if (interpretation.isPresent()) {
      files = MetricsMerge.mergeGenerated(files, interpretation.get().getFileInfos());
      indexeable = interpretation.get().isIndexeable();
    } else {
      indexeable = checklist.map(Metrics::isIndexeable).orElse(false);
    }
    return Metrics.builder().indexeable(indexeable).fileInfos(files).build();
  }

  private Optional<Metrics> report(UUID key, String type) {
    return workflows.report(key, WorkflowReader.ATTEMPT, type).map(r -> readMetrics(type, r));
  }

  private Metrics readMetrics(String type, String report) {
    try {
      // H2 returns a JSON value stored from a string as a quoted string; unwrap it.
      String text = report.startsWith("\"") ? json.readValue(report, String.class) : report;
      return json.readValue(text, Metrics.class);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Unreadable " + type + " report", e);
    }
  }

  static List<ValidationStep> stepTypes(List<WorkflowReader.Step> steps) {
    List<ValidationStep> result = new ArrayList<>();
    result.add(
        ValidationStep.builder()
            .stepType(StepType.VALIDATOR_UPLOAD_ARCHIVE.name())
            .status(Status.FINISHED)
            .executionOrder(0)
            .build());
    Optional<WorkflowReader.Step> checklistbank = checklistbank(steps);
    int order = 1;
    for (WorkflowReader.Step step : steps) {
      if (isChecklistbank(step)) {
        continue; // shown inside the archive step
      }
      Status status = stepStatus(step.status());
      String message = stepMessage(step);
      if (StepType.VALIDATOR_VALIDATE_ARCHIVE.name().equals(step.stepType())
          && "FINISHED".equals(step.status())
          && checklistbank.isPresent()) {
        status = checklistbankStatus(checklistbank.get().status());
        message = stepMessage(checklistbank.get());
      }
      result.add(
          ValidationStep.builder()
              .stepType(step.stepType())
              .status(status)
              .message(message)
              .executionOrder(order++)
              .build());
    }
    return result;
  }

  private static Optional<WorkflowReader.Step> checklistbank(List<WorkflowReader.Step> steps) {
    return steps.stream().filter(WorkflowStatusComposer::isChecklistbank).findFirst();
  }

  private static boolean isChecklistbank(WorkflowReader.Step step) {
    return StepType.VALIDATOR_CHECKLISTBANK.name().equals(step.stepType());
  }

  /** The archive step's status once its ChecklistBank wait has started. */
  private static Status checklistbankStatus(String engineStatus) {
    return switch (engineStatus) {
      case "QUEUED" -> Status.WAITING_FOR_CHECKLISTBANK;
      case "PENDING" -> Status.RUNNING; // between the archive check and the wait
      default -> stepStatus(engineStatus);
    };
  }

  /** Engine step statuses as validation statuses; PENDING has none (a future step). */
  static Status stepStatus(String engineStatus) {
    return switch (engineStatus) {
      case "QUEUED" -> Status.QUEUED;
      case "RUNNING" -> Status.RUNNING;
      case "FINISHED" -> Status.FINISHED;
      case "FAILED" -> Status.FAILED;
      case "ABORTED", "SKIPPED" -> Status.ABORTED;
      default -> null;
    };
  }

  private static String stepMessage(WorkflowReader.Step step) {
    if ("SKIPPED".equals(step.status())) {
      return step.reason() == null ? "Skipped" : "Skipped: " + step.reason();
    }
    return step.reason();
  }

  /**
   * The workflow's overall status while it runs: WAITING_FOR_CHECKLISTBANK during the wait,
   * RUNNING once a step runs, else QUEUED.
   */
  private static Status status(String workflowStatus, List<WorkflowReader.Step> steps) {
    return switch (workflowStatus) {
      case "FINISHED", "FAILED", "ABORTED" -> finalStatus(workflowStatus);
      default -> {
        if (checklistbank(steps).filter(s -> "QUEUED".equals(s.status())).isPresent()) {
          yield Status.WAITING_FOR_CHECKLISTBANK;
        }
        yield steps.stream().anyMatch(s -> "RUNNING".equals(s.status()))
            ? Status.RUNNING
            : Status.QUEUED;
      }
    };
  }
}
