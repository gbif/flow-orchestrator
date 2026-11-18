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
package org.gbif.orchestrator.app.persistence;

import org.gbif.api.model.pipelines.StepType;
import org.gbif.orchestrator.api.step.Outcome;
import org.gbif.orchestrator.engine.WorkflowState.Status;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/** A row of {@code step_attempt}: one try of a step. The attempts form the step's history. */
public record StepAttempt(
    UUID workflowExecutionId,
    StepType stepType,
    int attemptNo,
    Status status,
    Outcome outcome,
    String diagnostic,
    Map<String, String> outputs,
    Instant created,
    Instant started,
    Instant finished) {

  public StepAttempt {
    outputs = outputs == null ? Map.of() : Map.copyOf(outputs);
  }
}
