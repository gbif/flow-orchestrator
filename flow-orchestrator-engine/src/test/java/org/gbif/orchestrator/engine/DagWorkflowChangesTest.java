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
import org.gbif.orchestrator.engine.WorkflowState.Status;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DagWorkflowChangesTest {

  private static final StepType ANALYSE = StepType.VALIDATOR_VALIDATE_DWCDP;
  private static final StepType STAGE = StepType.VALIDATOR_DWCDP_STAGE;
  private static final StepType CONVERT = StepType.VALIDATOR_DWCDP_TO_VERBATIM;

  @Test
  void skipProposalExcludesBranchAndSchedulesRemainingStep() {
    DagDefinition dag =
        new DagDefinition(
            List.of(
                new DagDefinition.Node(ANALYSE, Set.of()),
                new DagDefinition.Node(STAGE, Set.of(ANALYSE)),
                new DagDefinition.Node(CONVERT, Set.of(ANALYSE))),
            DagDefinition.JoinPolicy.NON_ERROR,
            false);

    WorkflowState state =
        new WorkflowState(
            Status.RUNNING,
            List.of(
                new WorkflowState.Step(ANALYSE, 0, Status.QUEUED),
                new WorkflowState.Step(STAGE, 1, Status.PENDING),
                new WorkflowState.Step(CONVERT, 2, Status.PENDING)));

    WorkflowDecision decision =
        new DagWorkflowEngine(dag)
            .evaluate(state, WorkflowEvent.completed(ANALYSE), List.of(WorkflowChange.skip(STAGE)));

    assertTrue(
        decision.actions().stream()
            .anyMatch(
                action ->
                    action.kind() == WorkflowDecision.Kind.SET_STEP_STATUS
                        && action.step() == STAGE
                        && action.status() == Status.SKIPPED));
    assertTrue(
        decision.actions().stream()
            .anyMatch(
                action ->
                    action.kind() == WorkflowDecision.Kind.DISPATCH && action.step() == CONVERT));
  }

  @Test
  void skipFromSkipsStepAndEverythingDownstreamAndFinishesWorkflow() {
    StepType identify = StepType.VALIDATOR_VERBATIM_TO_IDENTIFIER;
    DagDefinition dag = DagDefinition.linear(List.of(ANALYSE, STAGE, CONVERT, identify));

    WorkflowState state =
        new WorkflowState(
            Status.RUNNING,
            List.of(
                new WorkflowState.Step(ANALYSE, 0, Status.QUEUED),
                new WorkflowState.Step(STAGE, 1, Status.PENDING),
                new WorkflowState.Step(CONVERT, 2, Status.PENDING),
                new WorkflowState.Step(identify, 3, Status.PENDING)));

    WorkflowDecision decision =
        new DagWorkflowEngine(dag)
            .evaluate(
                state, WorkflowEvent.completed(ANALYSE), List.of(WorkflowChange.skipFrom(STAGE)));

    for (StepType skipped : List.of(STAGE, CONVERT, identify)) {
      assertTrue(hasStepStatus(decision, skipped, Status.SKIPPED), skipped + " skipped");
    }
    assertTrue(
        decision.actions().stream()
            .noneMatch(action -> action.kind() == WorkflowDecision.Kind.DISPATCH));
    assertTrue(
        decision.actions().stream()
            .anyMatch(
                action ->
                    action.kind() == WorkflowDecision.Kind.SET_WORKFLOW_STATUS
                        && action.status() == Status.FINISHED));
  }

  @Test
  void skipFromLeavesParallelBranchRunning() {
    DagDefinition dag =
        new DagDefinition(
            List.of(
                new DagDefinition.Node(ANALYSE, Set.of()),
                new DagDefinition.Node(STAGE, Set.of(ANALYSE)),
                new DagDefinition.Node(CONVERT, Set.of(ANALYSE))),
            DagDefinition.JoinPolicy.NON_ERROR,
            true);

    WorkflowState state =
        new WorkflowState(
            Status.RUNNING,
            List.of(
                new WorkflowState.Step(ANALYSE, 0, Status.QUEUED),
                new WorkflowState.Step(STAGE, 1, Status.PENDING),
                new WorkflowState.Step(CONVERT, 2, Status.PENDING)));

    WorkflowDecision decision =
        new DagWorkflowEngine(dag)
            .evaluate(
                state, WorkflowEvent.completed(ANALYSE), List.of(WorkflowChange.skipFrom(STAGE)));

    assertTrue(hasStepStatus(decision, STAGE, Status.SKIPPED));
    assertTrue(
        decision.actions().stream()
            .anyMatch(
                action ->
                    action.kind() == WorkflowDecision.Kind.DISPATCH && action.step() == CONVERT));
  }

  @Test
  void overlappingSkipFromRangesAreAccepted() {
    DagDefinition dag = DagDefinition.linear(List.of(ANALYSE, STAGE, CONVERT));
    WorkflowState state =
        new WorkflowState(
            Status.RUNNING,
            List.of(
                new WorkflowState.Step(ANALYSE, 0, Status.QUEUED),
                new WorkflowState.Step(STAGE, 1, Status.PENDING),
                new WorkflowState.Step(CONVERT, 2, Status.PENDING)));

    WorkflowDecision decision =
        new DagWorkflowEngine(dag)
            .evaluate(
                state,
                WorkflowEvent.completed(ANALYSE),
                List.of(WorkflowChange.skipFrom(STAGE), WorkflowChange.skipFrom(CONVERT)));

    assertTrue(hasStepStatus(decision, CONVERT, Status.SKIPPED));
  }

  private static boolean hasStepStatus(WorkflowDecision decision, StepType step, Status status) {
    return decision.actions().stream()
        .anyMatch(
            action ->
                action.kind() == WorkflowDecision.Kind.SET_STEP_STATUS
                    && action.step() == step
                    && action.status() == status);
  }

  @Test
  void cannotSkipAlreadyQueuedStep() {
    DagDefinition dag =
        new DagDefinition(
            List.of(
                new DagDefinition.Node(ANALYSE, Set.of()),
                new DagDefinition.Node(STAGE, Set.of(ANALYSE))),
            DagDefinition.JoinPolicy.NON_ERROR,
            false);

    WorkflowState state =
        new WorkflowState(
            Status.RUNNING,
            List.of(
                new WorkflowState.Step(ANALYSE, 0, Status.QUEUED),
                new WorkflowState.Step(STAGE, 1, Status.QUEUED)));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new DagWorkflowEngine(dag)
                .evaluate(
                    state, WorkflowEvent.completed(ANALYSE), List.of(WorkflowChange.skip(STAGE))));
  }
}
