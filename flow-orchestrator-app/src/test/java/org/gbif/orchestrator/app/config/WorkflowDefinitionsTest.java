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
import org.gbif.orchestrator.api.command.Domain;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkflowDefinitionsTest {

  private static final StepType ANALYSE = StepType.VALIDATOR_VALIDATE_DWCDP;
  private static final StepType STAGE = StepType.VALIDATOR_DWCDP_STAGE;
  private static final StepType ARCHIVE = StepType.VALIDATOR_VALIDATE_ARCHIVE;

  @Test
  void resolvesByDomainAndParameters() {
    OrchestratorProperties properties = properties();
    workflow(properties, "dwcdp", Domain.VALIDATION, Map.of("fileFormat", "DWCDP"), ANALYSE, STAGE);
    workflow(properties, "dwca", Domain.VALIDATION, Map.of("fileFormat", "DWCA"), ARCHIVE);

    WorkflowDefinitions definitions = new WorkflowDefinitions(properties);

    WorkflowDefinition dwcdp =
        definitions
            .resolve(Domain.VALIDATION, Map.of("fileFormat", "DWCDP", "other", "x"))
            .orElseThrow();
    assertEquals("dwcdp", dwcdp.name());
    assertEquals(List.of(ANALYSE, STAGE), dwcdp.order());
    assertEquals(2, dwcdp.orderOf(STAGE));
    assertEquals(
        "dwca",
        definitions.resolve(Domain.VALIDATION, Map.of("fileFormat", "DWCA")).orElseThrow().name());
  }

  @Test
  void noMatchForOtherDomainOrParameters() {
    OrchestratorProperties properties = properties();
    workflow(properties, "dwcdp", Domain.VALIDATION, Map.of("fileFormat", "DWCDP"), ANALYSE);
    WorkflowDefinitions definitions = new WorkflowDefinitions(properties);

    assertTrue(definitions.resolve(Domain.INGESTION, Map.of("fileFormat", "DWCDP")).isEmpty());
    assertTrue(definitions.resolve(Domain.VALIDATION, Map.of("fileFormat", "XML")).isEmpty());
    assertTrue(definitions.resolve(Domain.VALIDATION, Map.of()).isEmpty());
  }

  @Test
  void overlappingMatchesResolveToNothing() {
    OrchestratorProperties properties = properties();
    workflow(properties, "any", Domain.VALIDATION, Map.of(), ARCHIVE);
    workflow(properties, "dwcdp", Domain.VALIDATION, Map.of("fileFormat", "DWCDP"), ANALYSE);
    WorkflowDefinitions definitions = new WorkflowDefinitions(properties);

    // Both match: a command must select exactly one workflow.
    assertTrue(definitions.resolve(Domain.VALIDATION, Map.of("fileFormat", "DWCDP")).isEmpty());
    assertEquals(
        "any",
        definitions.resolve(Domain.VALIDATION, Map.of("fileFormat", "XML")).orElseThrow().name());
  }

  @Test
  void identicalMatchesFailAtStartup() {
    OrchestratorProperties properties = properties();
    workflow(properties, "a", Domain.VALIDATION, Map.of("fileFormat", "DWCDP"), ANALYSE);
    workflow(properties, "b", Domain.VALIDATION, Map.of("fileFormat", "DWCDP"), STAGE);
    assertThrows(IllegalStateException.class, () -> new WorkflowDefinitions(properties));
  }

  @Test
  void missingRoutingFailsAtStartup() {
    OrchestratorProperties properties = properties();
    workflow(properties, "dwcdp", Domain.VALIDATION, Map.of(), ANALYSE);
    properties.getSteps().remove(ANALYSE);
    assertThrows(IllegalStateException.class, () -> new WorkflowDefinitions(properties));
  }

  @Test
  void invalidDagFailsAtStartup() {
    OrchestratorProperties properties = properties();
    OrchestratorProperties.Workflow workflow = workflow(properties, "dwcdp", Domain.VALIDATION, Map.of(), STAGE);
    workflow.getNodes().get(0).setDependencies(List.of(ANALYSE)); // unknown dependency
    assertThrows(IllegalStateException.class, () -> new WorkflowDefinitions(properties));
  }

  @Test
  void hashIsStableAndChangesWithTheDefinition() {
    OrchestratorProperties first = properties();
    workflow(first, "dwcdp", Domain.VALIDATION, Map.of("fileFormat", "DWCDP"), ANALYSE, STAGE);
    OrchestratorProperties same = properties();
    workflow(same, "renamed", Domain.VALIDATION, Map.of("fileFormat", "DWCDP"), ANALYSE, STAGE);
    OrchestratorProperties different = properties();
    workflow(different, "dwcdp", Domain.VALIDATION, Map.of("fileFormat", "DWCDP"), ANALYSE);

    String hash = new WorkflowDefinitions(first).all().get(0).hash();
    assertEquals(hash, new WorkflowDefinitions(same).all().get(0).hash());
    assertNotEquals(hash, new WorkflowDefinitions(different).all().get(0).hash());
    assertEquals(64, hash.length());
  }

  private static OrchestratorProperties properties() {
    OrchestratorProperties properties = new OrchestratorProperties();
    properties.setDomain(Domain.VALIDATION);
    for (StepType step : List.of(ANALYSE, STAGE, ARCHIVE)) {
      OrchestratorProperties.Step routing = new OrchestratorProperties.Step();
      routing.getRequest().setExchange("occurrence");
      routing.getRequest().setRoutingKey("route." + step.name());
      properties.getSteps().put(step, routing);
    }
    return properties;
  }

  /** Adds a linear workflow over the given steps. */
  private static OrchestratorProperties.Workflow workflow(
      OrchestratorProperties properties,
      String name,
      Domain domain,
      Map<String, String> match,
      StepType... steps) {
    OrchestratorProperties.Workflow workflow = new OrchestratorProperties.Workflow();
    workflow.setDomain(domain);
    workflow.setMatch(match);
    StepType previous = null;
    for (StepType step : steps) {
      OrchestratorProperties.Node node = new OrchestratorProperties.Node();
      node.setStep(step);
      node.setDependencies(previous == null ? List.of() : List.of(previous));
      workflow.getNodes().add(node);
      previous = step;
    }
    properties.getWorkflows().put(name, workflow);
    return workflow;
  }
}
