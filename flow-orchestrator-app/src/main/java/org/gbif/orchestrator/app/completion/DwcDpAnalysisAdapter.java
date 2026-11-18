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
import org.gbif.orchestrator.api.command.Domain;
import org.gbif.orchestrator.api.report.ReportTypes;
import org.gbif.orchestrator.api.step.Outcome;
import org.gbif.orchestrator.api.step.Report;
import org.gbif.orchestrator.api.step.StepCompleted;
import org.gbif.orchestrator.api.step.StepHeader;
import org.gbif.orchestrator.app.config.OrchestratorProperties;
import org.gbif.orchestrator.app.persistence.StepExecution;
import org.gbif.orchestrator.app.persistence.StepExecutionRepository;
import org.gbif.orchestrator.app.persistence.WorkflowExecution;
import org.gbif.orchestrator.app.persistence.WorkflowExecutionRepository;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Maps the dwc-dp-analyser-service's legacy completion, {@code DwcDpValidationFinished}
 * ({@code {datasetUuid, attempt, valid, validationReport}}), to a {@link StepCompleted} for the
 * analysis step, until the analyser adopts the envelope (Phase B).
 *
 * <p>The message carries no workflow or step attempt, so it is correlated by {@code datasetUuid +
 * attempt} to the latest run of that subject and the current attempt of its analysis step. The
 * usual stale check then drops redelivered and outdated messages. The analyser sends nothing when
 * it fails, so this is always {@code SUCCEEDED}; whether the data is valid is decided by the
 * analysis report interpreter.
 *
 * <p>{@code validationReport} is the analyser's {@code AnalysisExecution}
 * ({@code {result, metadata: {started, finished, features, isValid}, version}}) and is stored
 * unchanged as the {@code DWCDP_ANALYSIS} report.
 */
@Component
public class DwcDpAnalysisAdapter {

  private static final Logger LOG = LoggerFactory.getLogger(DwcDpAnalysisAdapter.class);

  private static final Map<Domain, StepType> ANALYSIS_STEPS =
      Map.of(Domain.VALIDATION, StepType.VALIDATOR_VALIDATE_DWCDP);

  private final WorkflowExecutionRepository executions;
  private final StepExecutionRepository steps;
  private final CompletionService completions;
  private final OrchestratorProperties properties;
  private final Clock clock;

  public DwcDpAnalysisAdapter(
      WorkflowExecutionRepository executions,
      StepExecutionRepository steps,
      CompletionService completions,
      OrchestratorProperties properties,
      Clock clock) {
    this.executions = executions;
    this.steps = steps;
    this.completions = completions;
    this.properties = properties;
    this.clock = clock;
  }

  /**
   * @throws IllegalArgumentException if the message is not a usable {@code DwcDpValidationFinished}
   * @throws UnknownWorkflowException if no workflow of this instance matches it
   */
  public CompletionResult handle(JsonNode message) {
    Instant received = clock.instant();
    if (message == null || !message.isObject()) {
      throw new IllegalArgumentException("Expected a DwcDpValidationFinished object");
    }
    UUID datasetKey = uuid(message.path("datasetUuid"));
    JsonNode attemptNode = message.path("attempt");
    if (!attemptNode.isIntegralNumber() || attemptNode.intValue() < 1) {
      throw new IllegalArgumentException("DwcDpValidationFinished has no valid attempt");
    }
    int attempt = attemptNode.intValue();
    JsonNode report = message.path("validationReport");
    if (!report.isObject()) {
      throw new IllegalArgumentException("DwcDpValidationFinished has no validationReport object");
    }

    Domain domain = properties.getDomain();
    StepType stepType = ANALYSIS_STEPS.get(domain);
    if (stepType == null) {
      throw new UnknownWorkflowException("No DwC-DP analysis step for domain " + domain);
    }
    WorkflowExecution execution =
        executions
            .findLatestRun(domain, datasetKey, attempt)
            .orElseThrow(
                () ->
                    new UnknownWorkflowException(
                        "No workflow for dataset " + datasetKey + " attempt " + attempt));
    StepExecution step =
        steps.findByExecution(execution.id()).stream()
            .filter(s -> s.stepType() == stepType)
            .findFirst()
            .orElseThrow(
                () ->
                    new UnknownWorkflowException(
                        "Workflow " + execution.id() + " has no step " + stepType));

    JsonNode metadata = report.path("metadata");
    StepCompleted completed =
        new StepCompleted(
            messageId(execution.id(), step.currentAttempt()),
            new StepHeader(execution.id(), datasetKey, attempt, stepType, step.currentAttempt()),
            Outcome.SUCCEEDED,
            null,
            Map.of(),
            List.of(new Report(ReportTypes.DWCDP_ANALYSIS, report, null)),
            instant(metadata.path("started"), received),
            instant(metadata.path("finished"), received));
    LOG.info(
        "DwC-DP analysis finished dataset={} attempt={} workflow={} stepAttempt={} valid={}",
        datasetKey,
        attempt,
        execution.id(),
        step.currentAttempt(),
        message.path("valid").asText("?"));
    return completions.handle(completed);
  }

  /** The same analysis of the same attempt always gets the same id. */
  private static UUID messageId(UUID workflowId, int stepAttempt) {
    return UUID.nameUUIDFromBytes(
        ("DwcDpValidationFinished/" + workflowId + "/" + stepAttempt)
            .getBytes(StandardCharsets.UTF_8));
  }

  private static UUID uuid(JsonNode node) {
    if (!node.isTextual()) {
      throw new IllegalArgumentException("DwcDpValidationFinished has no datasetUuid");
    }
    try {
      return UUID.fromString(node.textValue());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "DwcDpValidationFinished has an invalid datasetUuid: " + node.textValue(), e);
    }
  }

  /**
   * The analyser's times are UTC: an ISO {@code LocalDateTime} in its messages, or with an offset as
   * it writes them to registry. Timing is informative only, so anything else falls back.
   */
  static Instant instant(JsonNode node, Instant fallback) {
    if (!node.isTextual()) {
      return fallback;
    }
    String text = node.textValue();
    try {
      return OffsetDateTime.parse(text).toInstant();
    } catch (DateTimeParseException e) {
      try {
        return LocalDateTime.parse(text).toInstant(ZoneOffset.UTC);
      } catch (DateTimeParseException notLocal) {
        return fallback;
      }
    }
  }
}
