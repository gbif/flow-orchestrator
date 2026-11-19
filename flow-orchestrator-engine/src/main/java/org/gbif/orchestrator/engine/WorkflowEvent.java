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

public final class WorkflowEvent {

  public enum Kind {
    START,
    COMPLETED,
    FAILED,
    /** Ends the workflow: every unfinished step and the workflow become ABORTED. */
    CANCEL
  }

  private final Kind kind;
  private final StepType step;

  private WorkflowEvent(Kind kind, StepType step) {
    this.kind = kind;
    this.step = step;
  }

  public static WorkflowEvent start() {
    return new WorkflowEvent(Kind.START, null);
  }

  public static WorkflowEvent cancel() {
    return new WorkflowEvent(Kind.CANCEL, null);
  }

  public static WorkflowEvent completed(StepType step) {
    return new WorkflowEvent(Kind.COMPLETED, Objects.requireNonNull(step));
  }

  public static WorkflowEvent failed(StepType step) {
    return new WorkflowEvent(Kind.FAILED, Objects.requireNonNull(step));
  }

  public Kind kind() {
    return kind;
  }

  public StepType step() {
    return step;
  }
}
