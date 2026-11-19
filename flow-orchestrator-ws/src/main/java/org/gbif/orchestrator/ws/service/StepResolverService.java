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
package org.gbif.orchestrator.ws.service;

import org.gbif.api.model.pipelines.StepType;
import org.gbif.orchestrator.ws.api.FileFormat;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.springframework.stereotype.Service;

@Service
public class StepResolverService {

  public Set<StepType> resolve(FileFormat fileFormat) {
    Objects.requireNonNull(fileFormat, "fileFormat must not be null");

    return switch (fileFormat) {
      case COLDP -> Set.of(StepType.VALIDATOR_VALIDATE_ARCHIVE);

      case DWCDP -> new LinkedHashSet<>(
          List.of(
              StepType.VALIDATOR_VALIDATE_DWCDP,
              StepType.VALIDATOR_DWCDP_STAGE,
              StepType.VALIDATOR_DWCDP_TO_VERBATIM,
              StepType.VALIDATOR_VERBATIM_TO_IDENTIFIER,
              StepType.VALIDATOR_VERBATIM_TO_INTERPRETED,
              StepType.VALIDATOR_COLLECT_METRICS));

      case DWCA, TABULAR, SPREADSHEET -> standardPipeline(StepType.VALIDATOR_DWCA_TO_VERBATIM);

      case XML -> standardPipeline(StepType.VALIDATOR_XML_TO_VERBATIM);
    };
  }

  private Set<StepType> standardPipeline(StepType conversionStep) {
    return new LinkedHashSet<>(
        List.of(
            StepType.VALIDATOR_VALIDATE_ARCHIVE,
            conversionStep,
            StepType.VALIDATOR_VERBATIM_TO_IDENTIFIER,
            StepType.VALIDATOR_VERBATIM_TO_INTERPRETED,
            StepType.VALIDATOR_COLLECT_METRICS));
  }
}
