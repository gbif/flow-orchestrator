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

import org.gbif.orchestrator.api.command.Domain;
import org.gbif.orchestrator.engine.WorkflowState.Status;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * A row of {@code workflow_execution}, the aggregate root of a workflow's state.
 *
 * <p>{@code rejected} and {@code reason} are the verdict: a rejection is a business outcome (the
 * data is not acceptable) and only goes with {@code FINISHED}; {@code reason} says why the workflow
 * ended as it did, for people.
 */
public record WorkflowExecution(
    UUID id,
    Domain domain,
    UUID datasetKey,
    int attempt,
    int run,
    String workflowName,
    String definitionHash,
    Map<String, String> parameters,
    Status status,
    int version,
    Instant created,
    Instant modified,
    Instant finished,
    boolean rejected,
    String reason) {

  public WorkflowExecution {
    parameters = Map.copyOf(parameters);
  }
}
