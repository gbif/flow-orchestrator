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
package org.gbif.orchestrator.app.request;

import org.gbif.api.model.pipelines.StepType;
import org.gbif.orchestrator.api.step.StepHeader;
import org.gbif.orchestrator.app.persistence.WorkflowExecution;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * What a request factory may use: the dispatched step's header, the execution, and the outputs of
 * the steps that have finished, latest step first.
 *
 * <p>A worker never knows which step produced its input; the factory asks for an output by name
 * and gets the value of the latest finished step that reported it.
 */
public record StepContext(
    StepHeader header, WorkflowExecution execution, List<FinishedStep> finished) {

  public StepContext {
    Objects.requireNonNull(header, "header");
    Objects.requireNonNull(execution, "execution");
    finished = List.copyOf(finished);
  }

  /** The value of {@code key} from the latest finished step that reported it. */
  public Optional<String> output(String key) {
    return finished.stream()
        .map(step -> step.outputs().get(key))
        .filter(Objects::nonNull)
        .findFirst();
  }

  /**
   * Like {@link #output(String)}, but required.
   *
   * @throws MissingOutputException if no finished step reported {@code key}
   */
  public String requireOutput(String key) {
    return output(key)
        .orElseThrow(
            () ->
                new MissingOutputException(
                    "No finished step of workflow "
                        + execution.id()
                        + " reported output '"
                        + key
                        + "', needed by "
                        + header.stepType()));
  }

  /** {@link #requireOutput(String)} as a number. */
  public long requireLong(String key) {
    String value = requireOutput(key);
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw new MissingOutputException(
          "Output '" + key + "' of workflow " + execution.id() + " is not a number: " + value);
    }
  }

  /** A finished step's type and the outputs of its current attempt. */
  public record FinishedStep(StepType stepType, int stepOrder, Map<String, String> outputs) {
    public FinishedStep {
      outputs = outputs == null ? Map.of() : Map.copyOf(outputs);
    }
  }

  /** A request needs an output that no earlier step reported. */
  public static class MissingOutputException extends IllegalStateException {
    public MissingOutputException(String message) {
      super(message);
    }
  }
}
