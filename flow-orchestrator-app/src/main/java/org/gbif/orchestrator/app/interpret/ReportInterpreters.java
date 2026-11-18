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

import org.gbif.api.model.pipelines.StepType;
import org.gbif.orchestrator.api.command.Domain;
import org.gbif.orchestrator.api.step.Report;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;

/** Finds the interpreter for a domain and step type. */
@Component
public class ReportInterpreters {

  private final Map<String, ReportInterpreter> interpreters = new HashMap<>();

  public ReportInterpreters(List<ReportInterpreter> interpreters) {
    for (ReportInterpreter interpreter : interpreters) {
      ReportInterpreter previous =
          this.interpreters.put(key(interpreter.domain(), interpreter.stepType()), interpreter);
      if (previous != null) {
        throw new IllegalStateException(
            "Two report interpreters for "
                + interpreter.domain()
                + " "
                + interpreter.stepType());
      }
    }
  }

  /** The decision for a successful step; empty when it has no interpreter or nothing to reject. */
  public Optional<ReportInterpreter.Rejection> interpret(
      Domain domain, StepType stepType, List<Report> reports) {
    ReportInterpreter interpreter = interpreters.get(key(domain, stepType));
    return interpreter == null ? Optional.empty() : interpreter.interpret(reports);
  }

  private static String key(Domain domain, StepType stepType) {
    return domain + "/" + stepType;
  }

  /** The content of the report of {@code type}. */
  static JsonNode require(List<Report> reports, String type) {
    return reports.stream()
        .filter(report -> report.type().equals(type))
        .map(Report::content)
        .filter(content -> content != null && content.isObject())
        .findFirst()
        .orElseThrow(
            () ->
                new ReportInterpreter.UnreadableReportException(
                    "Missing " + type + " report (a JSON object)"));
  }

  /** A required boolean field, following a path of object fields. */
  static boolean requireBoolean(JsonNode node, String type, String... path) {
    JsonNode value = at(node, path);
    if (!value.isBoolean()) {
      throw new ReportInterpreter.UnreadableReportException(
          type + " report has no boolean " + String.join(".", path));
    }
    return value.booleanValue();
  }

  /** A required whole number field. */
  static long requireLong(JsonNode node, String type, String... path) {
    JsonNode value = at(node, path);
    if (!value.isIntegralNumber()) {
      throw new ReportInterpreter.UnreadableReportException(
          type + " report has no whole number " + String.join(".", path));
    }
    return value.longValue();
  }

  private static JsonNode at(JsonNode node, String... path) {
    JsonNode current = node;
    for (String field : path) {
      current = current.path(field);
    }
    return current;
  }
}
