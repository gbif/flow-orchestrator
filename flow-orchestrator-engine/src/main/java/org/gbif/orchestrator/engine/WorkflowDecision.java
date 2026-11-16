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

import java.util.List;
import java.util.Objects;

/** Ordered actions whose side effects are executed outside the workflow engine. */
public final class WorkflowDecision {

  public enum Kind {
    SET_STEP_STATUS,
    SET_WORKFLOW_STATUS,
    DISPATCH
  }

  public static final class Action {

    private final Kind kind;
    private final StepType step;
    private final WorkflowState.Status status;

    private Action(Kind kind, StepType step, WorkflowState.Status status) {
      this.kind = kind;
      this.step = step;
      this.status = status;
    }

    public static Action stepStatus(StepType step, WorkflowState.Status status) {
      return new Action(
          Kind.SET_STEP_STATUS, Objects.requireNonNull(step), Objects.requireNonNull(status));
    }

    public static Action workflowStatus(WorkflowState.Status status) {
      return new Action(Kind.SET_WORKFLOW_STATUS, null, Objects.requireNonNull(status));
    }

    public static Action dispatch(StepType step) {
      return new Action(Kind.DISPATCH, Objects.requireNonNull(step), null);
    }

    public Kind kind() {
      return kind;
    }

    public StepType step() {
      return step;
    }

    public WorkflowState.Status status() {
      return status;
    }
  }

  private final List<Action> actions;

  public WorkflowDecision(List<Action> actions) {
    this.actions = List.copyOf(actions);
  }

  public List<Action> actions() {
    return actions;
  }

  public boolean isNoop() {
    return actions.isEmpty();
  }
}
