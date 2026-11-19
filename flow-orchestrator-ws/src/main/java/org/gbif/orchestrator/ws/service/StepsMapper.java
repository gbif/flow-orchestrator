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

import org.gbif.api.model.pipelines.PipelinesWorkflow;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.orchestrator.ws.api.Metrics;
import org.gbif.orchestrator.ws.api.Metrics.ValidationStep;
import org.gbif.orchestrator.ws.api.Validation;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@UtilityClass
public class StepsMapper {

  /** Maps raw step types to ValidationSteps. */
  public static List<ValidationStep> mapToValidationSteps(Set<StepType> stepTypes) {

    List<ValidationStep> collect =
        stepTypes.stream()
            .filter(st -> st != StepType.VALIDATOR_UPLOAD_ARCHIVE)
            .map(
                st ->
                    Metrics.ValidationStep.builder()
                        .executionOrder(PipelinesWorkflow.getValidatorWorkflow().getLevel(st))
                        .stepType(st.name())
                        .build())
            .sorted(Comparator.comparingInt(ValidationStep::getExecutionOrder))
            .collect(Collectors.toList());

    collect.add(
        0,
        Metrics.ValidationStep.builder()
            .stepType(StepType.VALIDATOR_UPLOAD_ARCHIVE.name())
            .status(Validation.Status.FINISHED)
            .executionOrder(
                PipelinesWorkflow.getValidatorWorkflow()
                    .getLevel(StepType.VALIDATOR_UPLOAD_ARCHIVE))
            .build());

    return collect;
  }

  /**
   * Creates a list with Singleton list containing
   * Metrics.ValidationStep.StepType.VALIDATOR_UPLOAD_ARCHIVE.
   */
  public static List<Metrics.ValidationStep> getUploadingSteps(
      Validation.Status status, String message) {
    return Collections.singletonList(
        Metrics.ValidationStep.builder()
            .stepType(StepType.VALIDATOR_UPLOAD_ARCHIVE.name())
            .status(status)
            .message(message)
            .build());
  }
}
