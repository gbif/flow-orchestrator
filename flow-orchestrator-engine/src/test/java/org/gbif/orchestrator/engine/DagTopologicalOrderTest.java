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
import org.gbif.orchestrator.engine.DagDefinition.JoinPolicy;
import org.gbif.orchestrator.engine.DagDefinition.Node;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DagTopologicalOrderTest {

  private static final StepType A = StepType.VALIDATOR_UPLOAD_ARCHIVE;
  private static final StepType B = StepType.VALIDATOR_VALIDATE_DWCDP;
  private static final StepType C = StepType.VALIDATOR_VALIDATE_ARCHIVE;
  private static final StepType D = StepType.VALIDATOR_VERBATIM_TO_IDENTIFIER;

  @Test
  void linearKeepsChainOrder() {
    assertEquals(List.of(D, A, C), DagDefinition.linear(List.of(D, A, C)).topologicalOrder());
  }

  @Test
  void branchesAreOrderedByNameAndJoinComesLast() {
    // A -> {B, C} -> D; B and C become ready together and are ordered by name.
    DagDefinition dag =
        new DagDefinition(
            List.of(
                new Node(D, Set.of(B, C)),
                new Node(C, Set.of(A)),
                new Node(B, Set.of(A)),
                new Node(A, Set.of())),
            JoinPolicy.NON_ERROR,
            true);

    List<StepType> branches = B.name().compareTo(C.name()) < 0 ? List.of(B, C) : List.of(C, B);
    assertEquals(List.of(A, branches.get(0), branches.get(1), D), dag.topologicalOrder());
  }

  @Test
  void independentRootsAreOrderedByName() {
    DagDefinition dag =
        new DagDefinition(
            List.of(new Node(C, Set.of()), new Node(A, Set.of())), JoinPolicy.NON_ERROR, true);
    List<StepType> expected = A.name().compareTo(C.name()) < 0 ? List.of(A, C) : List.of(C, A);
    assertEquals(expected, dag.topologicalOrder());
  }
}
