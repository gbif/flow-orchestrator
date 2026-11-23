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
package org.gbif.orchestrator.app.interpret;

import org.gbif.orchestrator.api.report.ReportTypes;
import org.gbif.orchestrator.api.step.Report;
import org.gbif.orchestrator.app.json.ContractJson;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ArchiveValidationInterpreterTest {

  private final ContractJson json = new ContractJson();
  private final ArchiveValidationInterpreter interpreter = new ArchiveValidationInterpreter();

  @Test
  void validArchiveContinues() {
    assertEquals(Optional.empty(), interpreter.interpret(report("{\"valid\":true,\"files\":[]}")));
  }

  @Test
  void invalidArchiveIsRejectedWithTheWorkersReason() {
    assertEquals(
        "Occurrence core is empty",
        interpreter
            .interpret(
                report("{\"valid\":false,\"invalidationReason\":\"Occurrence core is empty\"}"))
            .orElseThrow()
            .reason());
  }

  @Test
  void invalidArchiveWithoutReasonGetsTheDefault() {
    assertEquals(
        ArchiveValidationInterpreter.DEFAULT_REASON,
        interpreter
            .interpret(report("{\"valid\":false,\"invalidationReason\":null}"))
            .orElseThrow()
            .reason());
  }

  @Test
  void reportWithoutVerdictIsUnreadable() {
    assertThrows(
        ReportInterpreter.UnreadableReportException.class,
        () -> interpreter.interpret(report("{\"files\":[]}")));
  }

  @Test
  void missingReportIsUnreadable() {
    assertThrows(
        ReportInterpreter.UnreadableReportException.class, () -> interpreter.interpret(List.of()));
  }

  private List<Report> report(String content) {
    return List.of(
        new Report(ReportTypes.ARCHIVE_VALIDATION, json.read(content, JsonNode.class), null));
  }
}
