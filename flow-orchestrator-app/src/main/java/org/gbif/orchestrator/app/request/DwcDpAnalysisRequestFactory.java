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
package org.gbif.orchestrator.app.request;

import org.gbif.api.model.pipelines.StepType;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.springframework.stereotype.Component;

/**
 * The dwc-dp-analyser-service's existing request, {@code {"datasetUuid", "attempt"}}. The analyser
 * replies with its legacy {@code DwcDpValidationFinished}, which the analysis adapter correlates
 * by the same two values.
 */
@Component
public class DwcDpAnalysisRequestFactory implements StepRequestFactory {

  @Override
  public Set<StepType> stepTypes() {
    return Set.of(StepType.VALIDATOR_VALIDATE_DWCDP);
  }

  @Override
  public Object create(StepContext context) {
    Map<String, Object> request = new LinkedHashMap<>();
    request.put("datasetUuid", context.header().datasetKey());
    request.put("attempt", context.header().attempt());
    return request;
  }
}
