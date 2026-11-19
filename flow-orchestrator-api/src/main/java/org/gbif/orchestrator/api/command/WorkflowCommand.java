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
package org.gbif.orchestrator.api.command;

import org.gbif.api.model.pipelines.StepType;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * A request to the orchestrator, sent by registry. One flat record for all command types; the
 * fields required by each {@link CommandType} are validated on construction.
 *
 * <p>The orchestrator records every command with its outcome (accepted or rejected with a reason).
 * Delivering the same {@code commandId} twice has no further effect.
 *
 * @param commandId idempotency key, unique per command
 * @param type what to do
 * @param domain workflow domain; required for {@link CommandType#START}
 * @param datasetKey subject of the workflow
 * @param attempt subject attempt, at least 1
 * @param step target step; required for {@link CommandType#RETRY_STEP} and {@link
 *     CommandType#RERUN_FROM}
 * @param parameters free-form parameters (for example {@code fileFormat}) used with the domain to
 *     select the workflow definition; never null
 * @param requestedBy user or system that issued the command
 * @param reason optional human-readable reason
 * @param requestedAt when the command was issued
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record WorkflowCommand(
    UUID commandId,
    CommandType type,
    Domain domain,
    UUID datasetKey,
    int attempt,
    StepType step,
    Map<String, String> parameters,
    String requestedBy,
    String reason,
    Instant requestedAt) {

  public WorkflowCommand {
    Objects.requireNonNull(commandId, "commandId");
    Objects.requireNonNull(type, "type");
    Objects.requireNonNull(datasetKey, "datasetKey");
    Objects.requireNonNull(requestedBy, "requestedBy");
    Objects.requireNonNull(requestedAt, "requestedAt");
    if (attempt < 1) {
      throw new IllegalArgumentException("attempt must be at least 1");
    }
    switch (type) {
      case START, CANCEL -> Objects.requireNonNull(domain, "domain is required for " + type);
      case RETRY_STEP, RERUN_FROM -> Objects.requireNonNull(step, "step is required for " + type);
      case RESTART -> {
        // No additional fields required.
      }
    }
    parameters = parameters == null ? Map.of() : Map.copyOf(parameters);
  }
}
