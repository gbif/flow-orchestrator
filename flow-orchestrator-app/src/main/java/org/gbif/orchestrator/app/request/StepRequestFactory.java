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

import java.util.Set;

/**
 * Builds the request message for dispatched steps of the given types. Called inside the decision
 * transaction; the result is serialized into the outbox. Routing (exchange, routing key) stays in
 * configuration.
 */
public interface StepRequestFactory {

  /** The step types this factory builds requests for. */
  Set<StepType> stepTypes();

  /**
   * @throws StepContext.MissingOutputException if an earlier step did not report a needed output
   */
  Object create(StepContext context);
}
