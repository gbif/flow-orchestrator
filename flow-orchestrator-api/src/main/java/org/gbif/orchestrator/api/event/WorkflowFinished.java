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
package org.gbif.orchestrator.api.event;

import org.gbif.orchestrator.api.command.Domain;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Published by the orchestrator when a workflow execution reaches a terminal state, for example so
 * registry can notify the user.
 *
 * <p>Deliberately contains no {@code StepType}: receivers may use an older gbif-api that does not
 * know every step type.
 *
 * @param workflowId workflow execution id
 * @param domain workflow domain
 * @param datasetKey subject of the workflow
 * @param attempt subject attempt, at least 1
 * @param result terminal result. {@code FAILED} means a technical error (our side); a business
 *     rejection of the data is {@code FINISHED} with {@code rejected} set
 * @param rejected true when the orchestrator rejected the data (for example an invalid data
 *     package), based on the step reports; only valid with {@code FINISHED}
 * @param reason human-readable reason for the outcome, for example why the data was rejected or
 *     why the workflow stopped early; optional
 * @param reportTypes types of the reports stored for this execution; never null
 * @param finishedAt when the workflow reached its terminal state
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record WorkflowFinished(
    UUID workflowId,
    Domain domain,
    UUID datasetKey,
    int attempt,
    WorkflowResult result,
    boolean rejected,
    String reason,
    List<String> reportTypes,
    Instant finishedAt) {

  public WorkflowFinished {
    Objects.requireNonNull(workflowId, "workflowId");
    Objects.requireNonNull(domain, "domain");
    Objects.requireNonNull(datasetKey, "datasetKey");
    Objects.requireNonNull(result, "result");
    Objects.requireNonNull(finishedAt, "finishedAt");
    if (attempt < 1) {
      throw new IllegalArgumentException("attempt must be at least 1");
    }
    if (rejected && result != WorkflowResult.FINISHED) {
      throw new IllegalArgumentException("rejected is only valid for result FINISHED");
    }
    reportTypes = reportTypes == null ? List.of() : List.copyOf(reportTypes);
  }
}
