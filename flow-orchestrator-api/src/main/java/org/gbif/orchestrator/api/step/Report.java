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

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * A human-facing result produced by a step. Each report is stored as one row, keyed by subject,
 * attempt and {@code type}.
 *
 * @param type report type, see {@link org.gbif.orchestrator.api.report.ReportTypes}
 * @param content report body as JSON
 * @param comment optional free text; the orchestrator fills in the producing step and attempt when
 *     absent
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Report(String type, JsonNode content, String comment) {

  public Report {
    if (type == null || type.isBlank()) {
      throw new IllegalArgumentException("type must not be blank");
    }
    Objects.requireNonNull(content, "content");
  }
}
