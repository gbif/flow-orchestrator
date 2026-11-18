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
package org.gbif.orchestrator.api;

import org.gbif.api.model.pipelines.StepType;
import org.gbif.orchestrator.api.command.CommandType;
import org.gbif.orchestrator.api.command.Domain;
import org.gbif.orchestrator.api.command.WorkflowCommand;
import org.gbif.orchestrator.api.event.WorkflowFinished;
import org.gbif.orchestrator.api.event.WorkflowResult;
import org.gbif.orchestrator.api.json.OrchestratorJson;
import org.gbif.orchestrator.api.report.ReportTypes;
import org.gbif.orchestrator.api.step.Outcome;
import org.gbif.orchestrator.api.step.Report;
import org.gbif.orchestrator.api.step.StepCompleted;
import org.gbif.orchestrator.api.step.StepHeader;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Locks the wire format against the golden fixtures in {@code src/test/resources/contracts}. Other
 * repositories depend on this format: change a fixture only together with a deliberate contract
 * change.
 */
class ContractJsonTest {

  private static final UUID DATASET = UUID.fromString("673f2308-3501-474f-85d5-a832825bb46c");
  private static final UUID WORKFLOW = UUID.fromString("6f1c2a3e-3333-4a8b-9c0d-000000000004");

  private final ObjectMapper mapper = OrchestratorJson.mapper();

  @Test
  void startCommand() throws IOException {
    WorkflowCommand expected =
        new WorkflowCommand(
            UUID.fromString("6f1c2a3e-1111-4a8b-9c0d-000000000001"),
            CommandType.START,
            Domain.VALIDATION,
            DATASET,
            1,
            null,
            Map.of("fileFormat", "DWCDP"),
            "jdoe",
            null,
            Instant.parse("2026-11-16T12:00:00Z"));
    assertMatchesFixture("command-start.json", expected, WorkflowCommand.class);
  }

  @Test
  void retryStepCommand() throws IOException {
    WorkflowCommand expected =
        new WorkflowCommand(
            UUID.fromString("6f1c2a3e-1111-4a8b-9c0d-000000000002"),
            CommandType.RETRY_STEP,
            null,
            DATASET,
            1,
            StepType.VALIDATOR_VALIDATE_DWCDP,
            null,
            "jdoe",
            "Analyser was down",
            Instant.parse("2026-11-16T12:30:00Z"));
    assertMatchesFixture("command-retry-step.json", expected, WorkflowCommand.class);
  }

  @Test
  void stepCompleted() throws IOException {
    assertMatchesFixture("step-completed.json", expectedStepCompleted(), StepCompleted.class);
  }

  @Test
  void workflowFinished() throws IOException {
    WorkflowFinished expected =
        new WorkflowFinished(
            WORKFLOW,
            Domain.VALIDATION,
            DATASET,
            1,
            WorkflowResult.FINISHED,
            true,
            "DwC-DP analysis: the data package is invalid",
            List.of(ReportTypes.DWCDP_ANALYSIS, ReportTypes.SUMMARY),
            Instant.parse("2026-11-16T12:01:11Z"));
    assertMatchesFixture("workflow-finished.json", expected, WorkflowFinished.class);
  }

  @Test
  void unknownFieldsAreIgnored() throws IOException {
    ObjectNode json = (ObjectNode) fixture("step-completed.json");
    json.put("addedInNewerVersion", "x");
    ((ObjectNode) json.get("header")).put("alsoNew", 42);
    ((ObjectNode) json.get("reports").get(0)).put("format", "v2");

    assertEquals(expectedStepCompleted(), mapper.treeToValue(json, StepCompleted.class));
  }

  @Test
  void unknownFieldsAreIgnoredWithoutHelperConfiguration() throws IOException {
    // Consumers may use their own mapper; the records themselves must tolerate newer senders.
    ObjectMapper plain = new ObjectMapper().findAndRegisterModules();
    ObjectNode json = (ObjectNode) fixture("workflow-finished.json");
    json.put("addedInNewerVersion", "x");

    assertEquals(
        mapper.treeToValue(fixture("workflow-finished.json"), WorkflowFinished.class),
        plain.treeToValue(json, WorkflowFinished.class));
  }

  private StepCompleted expectedStepCompleted() throws IOException {
    JsonNode analysis =
        mapper.readTree("{\"valid\":false,\"resourceAnalysisResults\":[{\"name\":\"occurrence\"}]}");
    return new StepCompleted(
        UUID.fromString("6f1c2a3e-2222-4a8b-9c0d-000000000003"),
        new StepHeader(WORKFLOW, DATASET, 1, StepType.VALIDATOR_VALIDATE_DWCDP, 2),
        // invalid data is a successful step; the orchestrator reads the report
        Outcome.SUCCEEDED,
        null,
        Map.of("datapackagePath", "/mnt/auto/crawler/dwcdp/673f2308-3501-474f-85d5-a832825bb46c/1"),
        List.of(new Report(ReportTypes.DWCDP_ANALYSIS, analysis, null)),
        Instant.parse("2026-11-16T12:00:05Z"),
        Instant.parse("2026-11-16T12:01:10Z"));
  }

  /** The fixture deserializes to {@code expected}, and {@code expected} serializes to the fixture. */
  private <T> void assertMatchesFixture(String name, T expected, Class<T> type) throws IOException {
    JsonNode fixture = fixture(name);
    assertEquals(expected, mapper.treeToValue(fixture, type), "deserialize " + name);
    assertEquals(fixture, mapper.valueToTree(expected), "serialize " + name);
  }

  private JsonNode fixture(String name) throws IOException {
    try (InputStream in = getClass().getResourceAsStream("/contracts/" + name)) {
      if (in == null) {
        throw new IOException("Missing fixture " + name);
      }
      return mapper.readTree(in);
    }
  }
}
