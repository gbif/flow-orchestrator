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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.FileSystemResource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The workflow definitions shipped as the chart's defaults ({@code helm/flow-orchestrator}
 * values, {@code config}) bind and pass the startup checks, and every validation upload the ws
 * starts selects exactly one of them.
 */
class ChartWorkflowsTest {

  private static final Path CHART_VALUES = Path.of("../helm/flow-orchestrator/values.yaml");

  @Test
  void chartWorkflowsLoadAndSelectOneWorkflowPerUpload() throws Exception {
    assertTrue(Files.isRegularFile(CHART_VALUES), "chart values at " + CHART_VALUES.toAbsolutePath());
    List<PropertySource<?>> sources =
        new YamlPropertySourceLoader().load("chart", new FileSystemResource(CHART_VALUES));
    OrchestratorProperties properties =
        new Binder(ConfigurationPropertySources.from(sources))
            .bind("config.orchestrator", OrchestratorProperties.class)
            .get();

    WorkflowDefinitions definitions = new WorkflowDefinitions(properties);

    assertEquals("validation-dwcdp", select(definitions, Map.of("fileFormat", "DWCDP")));
    assertEquals(
        "validation-dwca", select(definitions, Map.of("fileFormat", "DWCA", "checklist", "false")));
    assertEquals(
        "validation-dwca-checklist",
        select(definitions, Map.of("fileFormat", "DWCA", "checklist", "true")));
    assertEquals("validation-tabular", select(definitions, Map.of("fileFormat", "TABULAR")));
    assertEquals(
        "validation-spreadsheet", select(definitions, Map.of("fileFormat", "SPREADSHEET")));
    assertEquals("validation-coldp", select(definitions, Map.of("fileFormat", "COLDP")));
    // XML was never supported by validator-ws: no workflow.
    assertTrue(
        definitions.resolve(Domain.VALIDATION, Map.of("fileFormat", "XML")).isEmpty(), "XML");

    WorkflowDefinition checklist =
        definitions
            .resolve(Domain.VALIDATION, Map.of("fileFormat", "DWCA", "checklist", "true"))
            .orElseThrow();
    assertEquals(
        List.of(
            StepType.VALIDATOR_VALIDATE_ARCHIVE,
            StepType.VALIDATOR_CHECKLISTBANK,
            StepType.VALIDATOR_DWCA_TO_VERBATIM,
            StepType.VALIDATOR_VERBATIM_TO_IDENTIFIER,
            StepType.VALIDATOR_VERBATIM_TO_INTERPRETED,
            StepType.VALIDATOR_COLLECT_METRICS),
        checklist.order());
  }

  private static String select(WorkflowDefinitions definitions, Map<String, String> parameters) {
    return definitions
        .resolve(Domain.VALIDATION, parameters)
        .map(WorkflowDefinition::name)
        .orElseThrow(() -> new AssertionError("No single workflow for " + parameters));
  }
}
