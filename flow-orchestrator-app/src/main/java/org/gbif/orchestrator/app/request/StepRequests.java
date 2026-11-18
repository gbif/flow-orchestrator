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

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

/**
 * Chooses the request factory for a step type. A step without a specific factory gets the new
 * contract's request, {@code {"header": StepHeader}}, which workers that adopted the envelope
 * read; legacy workers have their own factory.
 */
@Component
public class StepRequests {

  private final Map<StepType, StepRequestFactory> factories = new EnumMap<>(StepType.class);

  public StepRequests(List<StepRequestFactory> factories) {
    for (StepRequestFactory factory : factories) {
      for (StepType type : factory.stepTypes()) {
        StepRequestFactory previous = this.factories.put(type, factory);
        if (previous != null) {
          throw new IllegalStateException(
              "Two request factories for "
                  + type
                  + ": "
                  + previous.getClass().getName()
                  + " and "
                  + factory.getClass().getName());
        }
      }
    }
  }

  public Object create(StepContext context) {
    StepRequestFactory factory = factories.get(context.header().stepType());
    return factory == null ? Map.of("header", context.header()) : factory.create(context);
  }
}
