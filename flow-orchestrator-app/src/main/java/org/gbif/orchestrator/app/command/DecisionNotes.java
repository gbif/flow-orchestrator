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
package org.gbif.orchestrator.app.command;

import org.gbif.api.model.pipelines.StepType;

import java.util.Map;

/**
 * What a decision means for people, stored next to the statuses it changes.
 *
 * @param rejected the orchestrator rejected the data; only allowed when the workflow ends {@code
 *     FINISHED}
 * @param workflowReason why the workflow ended (or changed) as it did; stored when the decision
 *     makes it terminal
 * @param stepReasons reasons for specific steps whose status this decision changes
 * @param defaultStepReason reason for steps this decision skips or aborts without an entry in
 *     {@code stepReasons}
 */
public record DecisionNotes(
    boolean rejected,
    String workflowReason,
    Map<StepType, String> stepReasons,
    String defaultStepReason) {

  public DecisionNotes {
    stepReasons = stepReasons == null ? Map.of() : Map.copyOf(stepReasons);
  }

  public static DecisionNotes none() {
    return new DecisionNotes(false, null, Map.of(), null);
  }
}
