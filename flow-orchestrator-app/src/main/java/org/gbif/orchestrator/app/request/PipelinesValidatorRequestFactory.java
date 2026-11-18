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
import org.gbif.common.messaging.api.messages.DwcDpStageMessage;
import org.gbif.common.messaging.api.messages.DwcDpToVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage.ValidationResult;
import org.gbif.orchestrator.api.step.OutputKeys;
import org.gbif.orchestrator.api.step.StepHeader;
import org.gbif.orchestrator.app.json.ContractJson;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;

/**
 * Requests for the pipelines DwC-DP validator jobs: the existing postal message with the step
 * header added, wrapped in a {@link PipelinesBalancerMessage} so the balancer still chooses
 * standalone or distributed. A job that finds a header replies to the orchestrator instead of
 * sending the next step's message.
 *
 * <p>{@code pipelineSteps} holds only the dispatched step: the jobs check that their step is in it,
 * and the verbatim routing key depends on whether VALIDATOR_VERBATIM_TO_IDENTIFIER is. No
 * data-shape flags are sent; the jobs read the shape from their input.
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
          case VALIDATOR_VERBATIM_TO_IDENTIFIER, VALIDATOR_VERBATIM_TO_INTERPRETED ->
              PipelinesVerbatimMessage.builder()
                  .datasetUuid(dataset)
                  .attempt(attempt)
                  .interpretTypes(INTERPRET_ALL)
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
                    INTERPRET_ALL,
                    datasetType(context));
            interpreted.setStepHeader(stepHeader);
            yield interpreted;
          }
          default -> throw new IllegalArgumentException("Not a pipelines validator step: " + header);
        };

    return new PipelinesBalancerMessage(message.getClass().getSimpleName(), message.toString());
  }

  /** As the DwC-DP to-verbatim job builds it today: identifiers are occurrenceIDs. */
  private static ValidationResult validationResult(StepContext context) {
    return new ValidationResult(
        false,
        true,
        false,
        context.requireLong(OutputKeys.OCCURRENCE_RECORDS),
        context.requireLong(OutputKeys.EVENT_RECORDS));
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
