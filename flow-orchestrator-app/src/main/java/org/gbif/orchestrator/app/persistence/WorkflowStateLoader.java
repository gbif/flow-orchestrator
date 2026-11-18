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
package org.gbif.orchestrator.app.persistence;

import org.gbif.orchestrator.engine.WorkflowState;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.stereotype.Component;

/** Builds the engine's view of a workflow from its persisted rows. */
@Component
public class WorkflowStateLoader {

  private final WorkflowExecutionRepository executions;
  private final StepExecutionRepository steps;

  public WorkflowStateLoader(WorkflowExecutionRepository executions, StepExecutionRepository steps) {
    this.executions = executions;
    this.steps = steps;
  }

  /** The execution row and its engine state, read in the caller's transaction. */
  public Loaded load(UUID workflowExecutionId) {
    return find(workflowExecutionId)
        .orElseThrow(
            () -> new IllegalStateException("Unknown workflow execution " + workflowExecutionId));
  }

  /** Like {@link #load(UUID)}, but empty for an unknown workflow execution. */
  public Optional<Loaded> find(UUID workflowExecutionId) {
    Optional<WorkflowExecution> found = executions.find(workflowExecutionId);
    if (found.isEmpty()) {
      return Optional.empty();
    }
    WorkflowExecution execution = found.get();
    List<StepExecution> stepRows = steps.findByExecution(workflowExecutionId);
    WorkflowState state =
        new WorkflowState(
            execution.status(),
            stepRows.stream()
                .map(s -> new WorkflowState.Step(s.stepType(), s.stepOrder(), s.status()))
                .toList());
    return Optional.of(new Loaded(execution, stepRows, state));
  }

  /** A loaded workflow: the root row (with its version), the step rows and the engine state. */
  public record Loaded(WorkflowExecution execution, List<StepExecution> steps, WorkflowState state) {
    public Loaded {
      steps = List.copyOf(steps);
    }
  }
}
