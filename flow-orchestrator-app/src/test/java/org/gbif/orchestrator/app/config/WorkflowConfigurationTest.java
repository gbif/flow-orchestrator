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
package org.gbif.orchestrator.app.config;

import org.gbif.api.model.pipelines.StepType;
import org.gbif.orchestrator.engine.DagDefinition;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkflowConfigurationTest {

  private final WorkflowConfiguration configuration = new WorkflowConfiguration();

  @Test
  void noNodesMeansNoDag() {
    assertTrue(configuration.dagDefinition(new DagProperties()).isEmpty());
  }

  @Test
  void buildsConfiguredDag() {
    DagProperties properties =
        properties(
            Map.of(
                StepType.VALIDATOR_VALIDATE_DWCDP, List.of(),
                StepType.VALIDATOR_DWCDP_STAGE, List.of(StepType.VALIDATOR_VALIDATE_DWCDP)));

    DagDefinition dag = configuration.dagDefinition(properties).orElseThrow();

    assertEquals(
        Set.of(StepType.VALIDATOR_VALIDATE_DWCDP),
        dag.require(StepType.VALIDATOR_DWCDP_STAGE).dependencies());
  }

  @Test
  void rejectsCycleAtStartup() {
    DagProperties properties =
        properties(
            Map.of(
                StepType.VALIDATOR_VALIDATE_DWCDP, List.of(StepType.VALIDATOR_DWCDP_STAGE),
                StepType.VALIDATOR_DWCDP_STAGE, List.of(StepType.VALIDATOR_VALIDATE_DWCDP)));

    assertThrows(IllegalArgumentException.class, () -> configuration.dagDefinition(properties));
  }

  @Test
  void rejectsUnknownDependencyAtStartup() {
    DagProperties properties =
        properties(
            Map.of(StepType.VALIDATOR_DWCDP_STAGE, List.of(StepType.VALIDATOR_VALIDATE_DWCDP)));

    assertThrows(IllegalArgumentException.class, () -> configuration.dagDefinition(properties));
  }

  private static DagProperties properties(Map<StepType, List<StepType>> dependencies) {
    Map<StepType, DagProperties.NodeProperties> nodes = new LinkedHashMap<>();
    dependencies.forEach(
        (step, deps) -> {
          DagProperties.NodeProperties node = new DagProperties.NodeProperties();
          node.setDependencies(deps);
          nodes.put(step, node);
        });
    DagProperties properties = new DagProperties();
    properties.setNodes(nodes);
    return properties;
  }
}
