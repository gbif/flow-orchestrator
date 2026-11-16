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
import org.gbif.orchestrator.engine.WorkflowDecision.Action;
import org.gbif.orchestrator.engine.WorkflowDecision.Kind;
import org.gbif.orchestrator.engine.WorkflowState.Status;

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Sequential workflows expressed as a linear DAG. Ported from the former {@code
 * SequentialWorkflowEngineTest}; deliberate behaviour differences are noted per test.
 */
class LinearDagTest {

  private static final StepType A = StepType.VALIDATOR_VALIDATE_DWCDP;
  private static final StepType B = StepType.VALIDATOR_DWCDP_STAGE;
  private static final StepType C = StepType.VALIDATOR_DWCDP_TO_VERBATIM;

  private final WorkflowEngine engine = new DagWorkflowEngine(DagDefinition.linear(List.of(A, B, C)));

  private WorkflowState state(Status status, Status a, Status b, Status c) {
    // Deliberately shuffled to verify execution order is taken from the step order.
    return new WorkflowState(
        status,
        List.of(
            new WorkflowState.Step(C, 30, c),
            new WorkflowState.Step(A, 10, a),
            new WorkflowState.Step(B, 20, b)));
  }

  @Test
  void linearBuildsChainOfDependencies() {
    DagDefinition dag = DagDefinition.linear(List.of(A, B, C));
    assertTrue(dag.require(A).dependencies().isEmpty());
    assertEquals(java.util.Set.of(A), dag.require(B).dependencies());
    assertEquals(java.util.Set.of(B), dag.require(C).dependencies());
    assertEquals(false, dag.parallel());
  }

  @Test
  void startQueuesFirstPendingStep() {
    WorkflowDecision result =
        engine.evaluate(
            state(Status.QUEUED, Status.PENDING, Status.PENDING, Status.PENDING),
            WorkflowEvent.start());
    assertEquals(
        List.of(Kind.SET_STEP_STATUS, Kind.SET_WORKFLOW_STATUS, Kind.DISPATCH),
        result.actions().stream().map(Action::kind).toList());
    assertEquals(A, result.actions().get(2).step());
    assertEquals(Status.QUEUED, result.actions().get(0).status());
  }

  @Test
  void successQueuesFollowingStep() {
    WorkflowDecision result =
        engine.evaluate(
            state(Status.RUNNING, Status.RUNNING, Status.PENDING, Status.PENDING),
            WorkflowEvent.completed(A));
    assertEquals(Status.FINISHED, result.actions().get(0).status());
    assertEquals(B, result.actions().get(1).step());
    assertEquals(Status.QUEUED, result.actions().get(1).status());
    assertEquals(Kind.DISPATCH, result.actions().get(3).kind());
    assertEquals(B, result.actions().get(3).step());
  }

  @Test
  void lastStepFinishesWorkflow() {
    WorkflowDecision result =
        engine.evaluate(
            state(Status.RUNNING, Status.FINISHED, Status.FINISHED, Status.RUNNING),
            WorkflowEvent.completed(C));
    assertEquals(2, result.actions().size());
    assertEquals(Kind.SET_WORKFLOW_STATUS, result.actions().get(1).kind());
    assertEquals(Status.FINISHED, result.actions().get(1).status());
  }

  /**
   * Behaviour change: the sequential engine left downstream steps PENDING; the DAG engine marks
   * them ABORTED so that every step reaches a terminal state.
   */
  @Test
  void failureAbortsDownstreamAndFailsWorkflow() {
    WorkflowDecision result =
        engine.evaluate(
            state(Status.RUNNING, Status.RUNNING, Status.PENDING, Status.PENDING),
            WorkflowEvent.failed(A));
    assertEquals(
        List.of(
            "SET_STEP_STATUS " + A + " FAILED",
            "SET_STEP_STATUS " + B + " ABORTED",
            "SET_STEP_STATUS " + C + " ABORTED",
            "SET_WORKFLOW_STATUS null FAILED"),
        result.actions().stream().map(LinearDagTest::describe).toList());
  }

  private static String describe(Action action) {
    return action.kind() + " " + action.step() + " " + action.status();
  }

  @Test
  void duplicateCompletionDoesNothing() {
    assertTrue(
        engine
            .evaluate(
                state(Status.RUNNING, Status.FINISHED, Status.QUEUED, Status.PENDING),
                WorkflowEvent.completed(A))
            .isNoop());
  }

  @Test
  void unexpectedCompletionDoesNothing() {
    assertTrue(
        engine
            .evaluate(
                state(Status.RUNNING, Status.RUNNING, Status.PENDING, Status.PENDING),
                WorkflowEvent.completed(B))
            .isNoop());
  }

  @Test
  void terminalWorkflowDoesNothing() {
    assertTrue(
        engine
            .evaluate(
                state(Status.ABORTED, Status.RUNNING, Status.PENDING, Status.PENDING),
                WorkflowEvent.completed(A))
            .isNoop());
  }

  @Test
  void neverDispatchesWhileStepIsActive() {
    assertTrue(
        engine
            .evaluate(
                state(Status.RUNNING, Status.QUEUED, Status.PENDING, Status.PENDING),
                WorkflowEvent.start())
            .isNoop());
  }

  /**
   * Behaviour change: the sequential engine ignored START on a snapshot with a failed step; the DAG
   * engine settles it by aborting downstream steps and failing the workflow. Nothing is dispatched.
   */
  @Test
  void neverDispatchesAfterFailedStep() {
    WorkflowDecision result =
        engine.evaluate(
            state(Status.RUNNING, Status.FAILED, Status.PENDING, Status.PENDING),
            WorkflowEvent.start());
    assertTrue(result.actions().stream().noneMatch(a -> a.kind() == Kind.DISPATCH));
    assertTrue(
        result.actions().stream()
            .anyMatch(a -> a.kind() == Kind.SET_WORKFLOW_STATUS && a.status() == Status.FAILED));
  }

  @Test
  void rejectsDuplicateStepOrders() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new WorkflowState(
                Status.QUEUED,
                List.of(
                    new WorkflowState.Step(A, 1, Status.PENDING),
                    new WorkflowState.Step(B, 1, Status.PENDING))));
  }
}
