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
package org.gbif.orchestrator.engine;

import java.util.List;

/** Computes the actions required to move a workflow forward. Implementations perform no I/O. */
public interface WorkflowEngine {

  /**
   * Evaluates an event against a workflow snapshot.
   *
   * @param changes declarative changes (for example skips) proposed for not-yet-started steps
   */
  WorkflowDecision evaluate(WorkflowState state, WorkflowEvent event, List<WorkflowChange> changes);

  default WorkflowDecision evaluate(WorkflowState state, WorkflowEvent event) {
    return evaluate(state, event, List.of());
  }
}
