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

class IdentifierValidationInterpreterTest {

  private final ContractJson json = new ContractJson();
  private final IdentifierValidationInterpreter interpreter = new IdentifierValidationInterpreter();

  @Test
  void validWithoutDuplicatesContinues() {
    assertEquals(Optional.empty(), interpreter.interpret(report(true, 0, "All good")));
  }

  @Test
  void invalidResultIsRejectedWithItsMessage() {
    assertEquals(
        "Too many records without identifiers",
        interpreter.interpret(report(false, 0, "Too many records without identifiers"))
            .orElseThrow()
            .reason());
  }

  @Test
  void duplicatesAreRejectedEvenWhenTheResultIsValid() {
    assertEquals(
        "Duplicate 3 identifiers found. Check field mapping.",
        interpreter.interpret(report(true, 3, "ok")).orElseThrow().reason());
  }

  @Test
  void reportWithoutVerdictIsUnreadable() {
    List<Report> reports =
        List.of(
            new Report(
                ReportTypes.IDENTIFIERS,
                json.read("{\"totalRecords\":10}", JsonNode.class),
                null));

    assertThrows(
        ReportInterpreter.UnreadableReportException.class, () -> interpreter.interpret(reports));
  }

  private List<Report> report(boolean valid, long duplicates, String message) {
    String content =
        String.format(
            "{\"totalRecords\":10,\"absentIdentifierRecords\":0,\"duplicateIdentifiers\":%d,"
                + "\"isResultValid\":%b,\"validationMessage\":\"%s\"}",
            duplicates, valid, message);
    return List.of(new Report(ReportTypes.IDENTIFIERS, json.read(content, JsonNode.class), null));
  }
}
