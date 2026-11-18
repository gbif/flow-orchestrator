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
 * Validation: identifier problems end the workflow rejected. The rule is the one the pipelines
 * identifier job applied itself before ({@code ValidatorIdentifierCallback.checkValidity}): an
 * invalid result, or any duplicate identifiers.
 */
@Component
public class IdentifierValidationInterpreter implements ReportInterpreter {

  @Override
  public Domain domain() {
    return Domain.VALIDATION;
  }

  @Override
  public StepType stepType() {
    return StepType.VALIDATOR_VERBATIM_TO_IDENTIFIER;
  }

  @Override
  public Optional<Rejection> interpret(List<Report> reports) {
    String type = ReportTypes.IDENTIFIERS;
    JsonNode report = ReportInterpreters.require(reports, type);
    boolean valid = ReportInterpreters.requireBoolean(report, type, "isResultValid");
    long duplicates = ReportInterpreters.requireLong(report, type, "duplicateIdentifiers");
    if (!valid) {
      String message = report.path("validationMessage").asText("");
      return Optional.of(
          new Rejection(message.isBlank() ? "The identifier validation failed" : message));
    }
    if (duplicates > 0) {
      return Optional.of(
          new Rejection(
              String.format("Duplicate %d identifiers found. Check field mapping.", duplicates)));
    }
    return Optional.empty();
  }
}
