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

import org.gbif.api.model.pipelines.StepType;

import java.util.Objects;

/** Request to exclude as-yet-unstarted steps from the active DAG. */
public record WorkflowChange(StepType step, Kind kind) {

  public enum Kind {
    /** Skip only this step; the join policy decides what happens to the steps after it. */
    SKIP,
    /** Skip this step and every step downstream of it. */
    SKIP_FROM
  }

  public WorkflowChange {
    Objects.requireNonNull(step, "step");
    Objects.requireNonNull(kind, "kind");
  }

  public static WorkflowChange skip(StepType step) {
    return new WorkflowChange(step, Kind.SKIP);
  }

  public static WorkflowChange skipFrom(StepType step) {
    return new WorkflowChange(step, Kind.SKIP_FROM);
  }
}
