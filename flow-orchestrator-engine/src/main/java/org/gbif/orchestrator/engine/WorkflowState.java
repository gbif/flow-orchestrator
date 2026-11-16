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

/** Immutable workflow snapshot. Step types and execution orders must be unique. */
public final class WorkflowState {

  public enum Status {
    PENDING,
    QUEUED,
    RUNNING,
    FINISHED,
    SKIPPED,
    FAILED,
    ABORTED
  }

  public static final class Step {

    private final StepType type;
    private final int order;
    private final Status status;

    public Step(StepType type, int order, Status status) {
      this.type = Objects.requireNonNull(type);
      this.order = order;
      this.status = Objects.requireNonNull(status);
    }

    public StepType type() {
      return type;
    }

    public int order() {
      return order;
    }

    public Status status() {
      return status;
    }
  }

  private final Status status;
  private final List<Step> steps;

  public WorkflowState(Status status, List<Step> steps) {
    this.status = Objects.requireNonNull(status);
    this.steps = List.copyOf(steps);

    if (this.steps.stream().map(Step::type).distinct().count() != this.steps.size()
        || this.steps.stream().map(Step::order).distinct().count() != this.steps.size()) {
      throw new IllegalArgumentException("Duplicate step types or execution orders");
    }
  }

  public Status status() {
    return status;
  }

  public List<Step> steps() {
    return steps;
  }
}
