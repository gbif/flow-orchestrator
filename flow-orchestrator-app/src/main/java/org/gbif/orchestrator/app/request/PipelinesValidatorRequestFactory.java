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
package org.gbif.orchestrator.app.request;

import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.DwcDpStageMessage;
import org.gbif.common.messaging.api.messages.DwcDpToVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage.ValidationResult;
import org.gbif.orchestrator.api.step.OutputKeys;
import org.gbif.orchestrator.api.step.StepHeader;
import org.gbif.orchestrator.app.json.ContractJson;

import java.net.URI;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;

/**
 * Requests for the pipelines validator jobs behind the balancer (the DwC-DP chain, DwC-A to
 * verbatim, and the shared identifier, interpretation and metrics steps): the existing postal
 * message with the step header added, wrapped in a {@link PipelinesBalancerMessage} so the
 * balancer still chooses standalone or distributed. A job that finds a header replies to the
 * orchestrator instead of sending the next step's message.
 *
 * <p>{@code pipelineSteps} holds only the dispatched step: the jobs check that their step is in it,
 * and the verbatim routing key depends on whether VALIDATOR_VERBATIM_TO_IDENTIFIER is. No
 * data-shape flags are sent; the jobs read the shape from their input.
 *
 * <p>The shared steps take the identifier flags and interpretation types from the conversion
 * step's outputs when it reported them (DwC-A); otherwise they get the DwC-DP values.
 */
@Component
public class PipelinesValidatorRequestFactory implements StepRequestFactory {

  private static final TypeReference<Map<String, Object>> MAP = new TypeReference<>() {};
  private static final Set<String> INTERPRET_ALL = Set.of("ALL");

  private final ContractJson json;

  public PipelinesValidatorRequestFactory(ContractJson json) {
    this.json = json;
  }

  @Override
  public Set<StepType> stepTypes() {
    return Set.of(
        StepType.VALIDATOR_DWCDP_STAGE,
        StepType.VALIDATOR_DWCDP_TO_VERBATIM,
        StepType.VALIDATOR_DWCA_TO_VERBATIM,
        StepType.VALIDATOR_VERBATIM_TO_IDENTIFIER,
        StepType.VALIDATOR_VERBATIM_TO_INTERPRETED,
        StepType.VALIDATOR_COLLECT_METRICS);
  }

  @Override
  public Object create(StepContext context) {
    StepHeader header = context.header();
    UUID dataset = header.datasetKey();
    int attempt = header.attempt();
    Set<String> steps = Set.of(header.stepType().name());
    Map<String, Object> stepHeader = json.mapper().convertValue(header, MAP);

    PipelineBasedMessage message =
        switch (header.stepType()) {
          case VALIDATOR_DWCDP_STAGE -> {
            DwcDpStageMessage stage =
                new DwcDpStageMessage(dataset, attempt, steps, null, false, false);
            stage.setStepHeader(stepHeader);
            yield stage;
          }
          case VALIDATOR_DWCDP_TO_VERBATIM -> {
            // The balancer sets standalone from the staged sizes.
            DwcDpToVerbatimMessage toVerbatim =
                new DwcDpToVerbatimMessage(dataset, attempt, steps, null, false, false, true);
            toVerbatim.setStepHeader(stepHeader);
            yield toVerbatim;
          }
          case VALIDATOR_DWCA_TO_VERBATIM -> {
            // The archive validator reported the core's shape; no crawler report exists here.
            PipelinesDwcaMessage dwca =
                new PipelinesDwcaMessage(
                    dataset,
                    datasetType(context),
                    URI.create("urn:gbif:validation:" + dataset),
                    attempt,
                    null,
                    steps,
                    EndpointType.DWC_ARCHIVE,
                    null,
                    null);
            dwca.setStepHeader(stepHeader);
            yield dwca;
          }
          case VALIDATOR_VERBATIM_TO_IDENTIFIER, VALIDATOR_VERBATIM_TO_INTERPRETED ->
              PipelinesVerbatimMessage.builder()
                  .datasetUuid(dataset)
                  .attempt(attempt)
                  .interpretTypes(interpretTypes(context))
                  .pipelineSteps(steps)
                  .validationResult(validationResult(context))
                  .datasetType(datasetType(context))
                  .stepHeader(stepHeader)
                  .build();
          case VALIDATOR_COLLECT_METRICS -> {
            ValidationResult result = validationResult(context);
            PipelinesInterpretedMessage interpreted =
                new PipelinesInterpretedMessage(
                    dataset,
                    attempt,
                    steps,
                    result.getNumberOfRecords(),
                    result.getNumberOfEventRecords(),
                    null,
                    false,
                    null,
                    null,
                    null,
                    result,
                    interpretTypes(context),
                    datasetType(context));
            interpreted.setStepHeader(stepHeader);
            yield interpreted;
          }
          default -> throw new IllegalArgumentException("Not a pipelines validator step: " + header);
        };

    return new PipelinesBalancerMessage(message.getClass().getSimpleName(), message.toString());
  }

  /**
   * Record counts from the conversion step. The identifier flags come from its outputs when it
   * reported them (DwC-A); otherwise as the DwC-DP to-verbatim job builds them: identifiers are
   * occurrenceIDs.
   */
  private static ValidationResult validationResult(StepContext context) {
    return new ValidationResult(
        flag(context, OutputKeys.TRIPLET_VALID, false),
        flag(context, OutputKeys.OCCURRENCE_ID_VALID, true),
        false,
        context.requireLong(OutputKeys.OCCURRENCE_RECORDS),
        context.requireLong(OutputKeys.EVENT_RECORDS));
  }

  private static boolean flag(StepContext context, String key, boolean otherwise) {
    return context
        .output(key)
        .map(
            value -> {
              if (!value.equals("true") && !value.equals("false")) {
                throw new StepContext.MissingOutputException(
                    "Output '" + key + "' is not true or false: " + value);
              }
              return Boolean.parseBoolean(value);
            })
        .orElse(otherwise);
  }

  /** The conversion step's interpretation types (comma-separated), or {@code ALL}. */
  private static Set<String> interpretTypes(StepContext context) {
    return context
        .output(OutputKeys.INTERPRET_TYPES)
        .map(
            value ->
                Arrays.stream(value.split(","))
                    .map(String::trim)
                    .filter(type -> !type.isEmpty())
                    .collect(Collectors.toUnmodifiableSet()))
        .filter(types -> !types.isEmpty())
        .orElse(INTERPRET_ALL);
  }

  private static DatasetType datasetType(StepContext context) {
    String value = context.requireOutput(OutputKeys.DATASET_TYPE);
    try {
      return DatasetType.valueOf(value);
    } catch (IllegalArgumentException e) {
      throw new StepContext.MissingOutputException(
          "Output '" + OutputKeys.DATASET_TYPE + "' is not a DatasetType: " + value);
    }
  }
}
