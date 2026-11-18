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

/**
 * Validation: an invalid data package ends the workflow rejected. Reads {@code metadata.isValid}
 * of the {@code DWCDP_ANALYSIS} report, the analyser's {@code AnalysisExecution}
 * ({@code {result, metadata: {started, finished, features, isValid}, version}}).
 */
@Component
public class DwcDpAnalysisInterpreter implements ReportInterpreter {

  @Override
  public Domain domain() {
    return Domain.VALIDATION;
  }

  @Override
  public StepType stepType() {
    return StepType.VALIDATOR_VALIDATE_DWCDP;
  }

  @Override
  public Optional<Rejection> interpret(List<Report> reports) {
    boolean valid =
        ReportInterpreters.requireBoolean(
            ReportInterpreters.require(reports, ReportTypes.DWCDP_ANALYSIS),
            ReportTypes.DWCDP_ANALYSIS,
            "metadata",
            "isValid");
    return valid
        ? Optional.empty()
        : Optional.of(new Rejection("The DwC-DP analysis found the data package invalid"));
  }
}
