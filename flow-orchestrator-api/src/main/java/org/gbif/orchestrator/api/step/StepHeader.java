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
package org.gbif.orchestrator.api.step;

import org.gbif.api.model.pipelines.StepType;

import java.util.Objects;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Identifies one step attempt. The orchestrator includes it in every step request, and the worker
 * copies it back unchanged into its {@link StepCompleted}.
 *
 * @param workflowId workflow execution id
 * @param datasetKey subject of the workflow; also used for logs and storage paths
 * @param attempt subject attempt, at least 1
 * @param stepType the step being executed
 * @param stepAttempt the orchestrator's attempt counter for this step, at least 1; completions for
 *     an attempt that is no longer current are ignored
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record StepHeader(
    UUID workflowId, UUID datasetKey, int attempt, StepType stepType, int stepAttempt) {

  public StepHeader {
    Objects.requireNonNull(workflowId, "workflowId");
    Objects.requireNonNull(datasetKey, "datasetKey");
    Objects.requireNonNull(stepType, "stepType");
    if (attempt < 1) {
      throw new IllegalArgumentException("attempt must be at least 1");
    }
    if (stepAttempt < 1) {
      throw new IllegalArgumentException("stepAttempt must be at least 1");
    }
  }
}
