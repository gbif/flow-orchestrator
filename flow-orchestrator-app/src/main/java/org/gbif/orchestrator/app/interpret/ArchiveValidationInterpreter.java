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
package org.gbif.orchestrator.app.interpret;

import org.gbif.api.model.pipelines.StepType;
import org.gbif.orchestrator.api.command.Domain;
import org.gbif.orchestrator.api.report.ReportTypes;
import org.gbif.orchestrator.api.step.Report;

import java.util.List;
import java.util.Optional;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Validation: an archive with a fatal issue, or without a usable occurrence core, ends the workflow
 * rejected (architecture §4a; the legacy validator failed the validation instead). Reads {@code
 * valid} and {@code invalidationReason} of the {@code ARCHIVE_VALIDATION} report.
 */
@Component
public class ArchiveValidationInterpreter implements ReportInterpreter {

  static final String DEFAULT_REASON = "The archive validation found the archive invalid";

  @Override
  public Domain domain() {
    return Domain.VALIDATION;
  }

  @Override
  public StepType stepType() {
    return StepType.VALIDATOR_VALIDATE_ARCHIVE;
  }

  @Override
  public Optional<Rejection> interpret(List<Report> reports) {
    JsonNode report = ReportInterpreters.require(reports, ReportTypes.ARCHIVE_VALIDATION);
    if (ReportInterpreters.requireBoolean(report, ReportTypes.ARCHIVE_VALIDATION, "valid")) {
      return Optional.empty();
    }
    JsonNode reason = report.get("invalidationReason");
    return Optional.of(
        new Rejection(
            reason != null && reason.isTextual() && !reason.asText().isBlank()
                ? reason.asText()
                : DEFAULT_REASON));
  }
}
