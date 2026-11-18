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

import java.util.List;
import java.util.Optional;

/**
 * Turns the reports of a successful step into a decision (section 4a): steps report facts, the
 * orchestrator decides. One interpreter per domain and step type; steps without one just continue.
 * The same report can mean different things in different domains.
 */
public interface ReportInterpreter {

  Domain domain();

  StepType stepType();

  /**
   * @param reports the reports of the completion, as stored
   * @return a rejection, or empty to continue
   * @throws UnreadableReportException if a needed report or field is missing or has the wrong
   *     shape; the step is then handled as failed, never as "continue"
   */
  Optional<Rejection> interpret(List<Report> reports);

  /** The data is not acceptable: the rest of the workflow is skipped and it ends rejected. */
  record Rejection(String reason) {
    public Rejection {
      if (reason == null || reason.isBlank()) {
        throw new IllegalArgumentException("A rejection needs a reason");
      }
    }
  }

  /** A report needed for the decision cannot be read. */
  class UnreadableReportException extends RuntimeException {
    public UnreadableReportException(String message) {
      super(message);
    }
  }
}
