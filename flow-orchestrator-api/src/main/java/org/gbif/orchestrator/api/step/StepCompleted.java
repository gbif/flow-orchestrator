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

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Sent by a worker when a step attempt ends, whatever the outcome. The orchestrator owns all status
 * transitions; the worker only reports what happened.
 *
 * @param messageId deduplication key for redelivered messages
 * @param header copied unchanged from the step request
 * @param outcome what happened to the work
 * @param diagnostic reason for the outcome; required unless {@link Outcome#SUCCEEDED}
 * @param outputs small machine-readable values for later steps (paths, counts, flags); never null
 * @param reports human-facing results, at most one per report type; never null
 * @param startedAt when the worker started the work
 * @param finishedAt when the worker finished the work
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record StepCompleted(
    UUID messageId,
    StepHeader header,
    Outcome outcome,
    String diagnostic,
    Map<String, String> outputs,
    List<Report> reports,
    Instant startedAt,
    Instant finishedAt) {

  public StepCompleted {
    Objects.requireNonNull(messageId, "messageId");
    Objects.requireNonNull(header, "header");
    Objects.requireNonNull(outcome, "outcome");
    Objects.requireNonNull(startedAt, "startedAt");
    Objects.requireNonNull(finishedAt, "finishedAt");
    if (outcome != Outcome.SUCCEEDED && (diagnostic == null || diagnostic.isBlank())) {
      throw new IllegalArgumentException("diagnostic is required for outcome " + outcome);
    }
    outputs = outputs == null ? Map.of() : Map.copyOf(outputs);
    reports = reports == null ? List.of() : List.copyOf(reports);
    Set<String> types = new HashSet<>();
    for (Report report : reports) {
      if (!types.add(report.type())) {
        throw new IllegalArgumentException("Duplicate report type: " + report.type());
      }
    }
  }
}
