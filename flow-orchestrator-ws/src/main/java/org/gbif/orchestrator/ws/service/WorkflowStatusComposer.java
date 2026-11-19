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

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Builds the {@code status} and {@code metrics} of an orchestrated validation from its workflow,
 * in the shape validator-ws stored them, so the data-validator UI reads them unchanged:
 *
 * <ul>
 *   <li>{@code stepTypes}: VALIDATOR_UPLOAD_ARCHIVE (finished, order 0), then every workflow step
 *       in its order, with its status and reason. Not-yet-started steps have no status, as before.
 *   <li>{@code files} and {@code indexeable}: from the {@code INTERPRETATION_METRICS} report (the
 *       collect-metrics step's output, which has the same JSON shape).
 *   <li>{@code error}: the workflow's reason when it failed or rejected the data.
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

    Metrics metrics = interpretationMetrics(validation).orElseGet(() -> Metrics.builder().build());
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

  private Optional<Metrics> interpretationMetrics(Validation validation) {
    return workflows
        .report(validation.getKey(), 1, ReportTypes.INTERPRETATION_METRICS)
        .map(this::readMetrics);
  }

  private Metrics readMetrics(String report) {
    try {
      // H2 returns a JSON value stored from a string as a quoted string; unwrap it.
      String text = report.startsWith("\"") ? json.readValue(report, String.class) : report;
      return json.readValue(text, Metrics.class);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Unreadable INTERPRETATION_METRICS report", e);
    }
  }

  private static List<ValidationStep> stepTypes(List<WorkflowReader.Step> steps) {
    List<ValidationStep> result = new ArrayList<>();
    result.add(
        ValidationStep.builder()
            .stepType(StepType.VALIDATOR_UPLOAD_ARCHIVE.name())
            .status(Status.FINISHED)
            .executionOrder(0)
            .build());
    for (WorkflowReader.Step step : steps) {
      result.add(
          ValidationStep.builder()
              .stepType(step.stepType())
              .status(stepStatus(step.status()))
              .message(stepMessage(step))
              .executionOrder(step.stepOrder() + 1)
              .build());
    }
    return result;
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

  /** The workflow's overall status while it runs: RUNNING once a step runs, else QUEUED. */
  private static Status status(String workflowStatus, List<WorkflowReader.Step> steps) {
    return switch (workflowStatus) {
      case "FINISHED", "FAILED", "ABORTED" -> finalStatus(workflowStatus);
      default ->
          steps.stream().anyMatch(s -> "RUNNING".equals(s.status()))
              ? Status.RUNNING
              : Status.QUEUED;
    };
  }
}
