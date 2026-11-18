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

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * A row of {@code workflow_report}: one report per subject, attempt and report type. Partial
 * reports come from steps; the summary is built by the orchestrator.
 *
 * @param datasetKey dataset key, or the validation key for validations
 * @param attempt subject attempt
 * @param reportType one of {@code ReportTypes}
 * @param report the report JSON
 * @param comment free text for people, for example the producing step and attempt
 * @param created when the report was first stored
 * @param modified when the report was last replaced
 */
public record WorkflowReport(
    UUID datasetKey,
    int attempt,
    String reportType,
    JsonNode report,
    String comment,
    Instant created,
    Instant modified) {

  public WorkflowReport {
    Objects.requireNonNull(datasetKey, "datasetKey");
    Objects.requireNonNull(reportType, "reportType");
    Objects.requireNonNull(report, "report");
  }
}
