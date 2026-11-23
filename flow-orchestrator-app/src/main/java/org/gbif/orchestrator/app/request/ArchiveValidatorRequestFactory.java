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
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.orchestrator.api.step.StepHeader;
import org.gbif.orchestrator.app.json.ContractJson;

import java.util.Map;
import java.util.Set;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;

/**
 * The pipelines archive validator's existing request, {@link PipelinesArchiveValidatorMessage},
 * with the step header added. It goes straight to the validator's queue (the balancer has no
 * handler for it); the worker validates the archive in the validation store and replies to the
 * orchestrator.
 *
 * <p>The file format comes from the START command's {@code fileFormat} parameter, which the
 * validation web service sets from the upload. The message is passed on as postal serializes it.
 */
@Component
public class ArchiveValidatorRequestFactory implements StepRequestFactory {

  /** The START parameter naming the upload's format (validation web service {@code FileFormat}). */
  public static final String FILE_FORMAT = "fileFormat";

  private static final TypeReference<Map<String, Object>> MAP = new TypeReference<>() {};

  private final ContractJson json;

  public ArchiveValidatorRequestFactory(ContractJson json) {
    this.json = json;
  }

  @Override
  public Set<StepType> stepTypes() {
    return Set.of(StepType.VALIDATOR_VALIDATE_ARCHIVE);
  }

  @Override
  public Object create(StepContext context) {
    StepHeader header = context.header();
    String fileFormat = context.execution().parameters().get(FILE_FORMAT);
    if (fileFormat == null || fileFormat.isBlank()) {
      throw new IllegalStateException(
          "The workflow has no '" + FILE_FORMAT + "' parameter for the archive validator: " + header);
    }
    PipelinesArchiveValidatorMessage message =
        new PipelinesArchiveValidatorMessage(
                header.datasetKey(),
                header.attempt(),
                Set.of(header.stepType().name()),
                null,
                fileFormat)
            .setStepHeader(json.mapper().convertValue(header, MAP));
    try {
      return json.mapper().readTree(message.toString());
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Postal wrote unreadable JSON for " + header, e);
    }
  }
}
