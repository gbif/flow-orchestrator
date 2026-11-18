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
import org.gbif.orchestrator.api.step.Outcome;
import org.gbif.orchestrator.api.step.Report;
import org.gbif.orchestrator.api.step.StepCompleted;
import org.gbif.orchestrator.api.step.StepHeader;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Validation rules enforced when a contract is constructed, on both sending and receiving side. */
class ContractRulesTest {

  private static final Instant NOW = Instant.parse("2026-11-16T12:00:00Z");
  private static final StepHeader HEADER =
      new StepHeader(UUID.randomUUID(), UUID.randomUUID(), 1, StepType.VALIDATOR_VALIDATE_DWCDP, 1);

  @Test
  void startRequiresDomain() {
    assertThrows(NullPointerException.class, () -> command(CommandType.START, null, null));
  }

  @Test
  void retryAndRerunRequireStep() {
    assertThrows(
        NullPointerException.class, () -> command(CommandType.RETRY_STEP, Domain.VALIDATION, null));
    assertThrows(
        NullPointerException.class, () -> command(CommandType.RERUN_FROM, Domain.VALIDATION, null));
  }

  @Test
  void restartNeedsNoDomainOrStep() {
    assertEquals(Map.of(), command(CommandType.RESTART, null, null).parameters());
  }

  @Test
  void commandRejectsAttemptBelowOne() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new WorkflowCommand(
                UUID.randomUUID(),
                CommandType.RESTART,
                null,
                UUID.randomUUID(),
                0,
                null,
                null,
                "jdoe",
                null,
                NOW));
  }

  @Test
  void commandParametersAreCopied() {
    Map<String, String> parameters = new HashMap<>(Map.of("fileFormat", "DWCA"));
    WorkflowCommand command =
        new WorkflowCommand(
            UUID.randomUUID(),
            CommandType.START,
            Domain.VALIDATION,
            UUID.randomUUID(),
            1,
            null,
            parameters,
            "jdoe",
            null,
            NOW);
    parameters.put("fileFormat", "XML");
    assertEquals("DWCA", command.parameters().get("fileFormat"));
  }

  @Test
  void headerRejectsAttemptsBelowOne() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new StepHeader(UUID.randomUUID(), UUID.randomUUID(), 0, StepType.VALIDATOR_VALIDATE_DWCDP, 1));
    assertThrows(
        IllegalArgumentException.class,
        () -> new StepHeader(UUID.randomUUID(), UUID.randomUUID(), 1, StepType.VALIDATOR_VALIDATE_DWCDP, 0));
  }

  @Test
  void failedOutcomeRequiresDiagnostic() {
    assertThrows(IllegalArgumentException.class, () -> completed(Outcome.FAILED, null, List.of()));
    assertThrows(IllegalArgumentException.class, () -> completed(Outcome.FAILED, " ", List.of()));
  }

  @Test
  void successNeedsNoDiagnosticAndDefaultsCollections() {
    StepCompleted completed = completed(Outcome.SUCCEEDED, null, null);
    assertTrue(completed.outputs().isEmpty());
    assertTrue(completed.reports().isEmpty());
  }

  @Test
  void duplicateReportTypesAreRejected() {
    List<Report> reports = List.of(report("DWCDP_ANALYSIS"), report("DWCDP_ANALYSIS"));
    assertThrows(
        IllegalArgumentException.class, () -> completed(Outcome.SUCCEEDED, null, reports));
  }

  @Test
  void reportsAreCopied() {
    List<Report> reports = new ArrayList<>(List.of(report("A")));
    StepCompleted completed = completed(Outcome.SUCCEEDED, null, reports);
    reports.add(report("B"));
    assertEquals(1, completed.reports().size());
  }

  @Test
  void reportRequiresTypeAndContent() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new Report(" ", JsonNodeFactory.instance.objectNode(), null));
    assertThrows(NullPointerException.class, () -> new Report("A", null, null));
  }

  @Test
  void rejectedOnlyForFinishedWorkflows() {
    // a rejection is a business outcome, not a technical failure
    assertThrows(IllegalArgumentException.class, () -> finished(WorkflowResult.FAILED, true));
    assertThrows(IllegalArgumentException.class, () -> finished(WorkflowResult.ABORTED, true));
    assertTrue(finished(WorkflowResult.FINISHED, true).rejected());
  }

  @Test
  void outcomeHasNoRejection() {
    assertEquals(List.of(Outcome.SUCCEEDED, Outcome.FAILED), List.of(Outcome.values()));
  }

  private static WorkflowFinished finished(WorkflowResult result, boolean rejected) {
    return new WorkflowFinished(
        UUID.randomUUID(),
        Domain.VALIDATION,
        UUID.randomUUID(),
        1,
        result,
        rejected,
        "reason",
        null,
        NOW);
  }

  private static WorkflowCommand command(CommandType type, Domain domain, StepType step) {
    return new WorkflowCommand(
        UUID.randomUUID(), type, domain, UUID.randomUUID(), 1, step, null, "jdoe", null, NOW);
  }

  private static StepCompleted completed(Outcome outcome, String diagnostic, List<Report> reports) {
    return new StepCompleted(
        UUID.randomUUID(), HEADER, outcome, diagnostic, null, reports, NOW, NOW);
  }

  private static Report report(String type) {
    return new Report(type, JsonNodeFactory.instance.objectNode(), null);
  }
}
