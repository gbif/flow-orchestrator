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
import org.gbif.orchestrator.api.step.OutputKeys;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.springframework.stereotype.Component;

/**
 * The wait for a ChecklistBank import: {@code {"header": StepHeader, "clbDatasetKey": n}}, consumed
 * by the validation web service. The ws records the wait; when ChecklistBank calls back (or its
 * poller sees the import finish) it hands the import, with this header, to the pipelines checklist
 * validator, which evaluates it and replies to the orchestrator.
 */
@Component
public class ChecklistbankRequestFactory implements StepRequestFactory {

  @Override
  public Set<StepType> stepTypes() {
    return Set.of(StepType.VALIDATOR_CHECKLISTBANK);
  }

  @Override
  public Object create(StepContext context) {
    Map<String, Object> request = new LinkedHashMap<>();
    request.put("header", context.header());
    request.put(OutputKeys.CLB_DATASET_KEY, context.requireLong(OutputKeys.CLB_DATASET_KEY));
    return request;
  }
}
