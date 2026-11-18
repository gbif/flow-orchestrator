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
import org.gbif.orchestrator.engine.DagDefinition;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A validated, named workflow definition.
 *
 * @param name configuration key of the workflow
 * @param domain domain the workflow belongs to
 * @param match command parameters that select this workflow
 * @param dag the DAG
 * @param hash SHA-256 of the canonical definition; recorded on each execution so a configuration
 *     change is visible
 * @param order deterministic topological step order, stored as {@code step_order}
 */
public record WorkflowDefinition(
    String name,
    Domain domain,
    Map<String, String> match,
    DagDefinition dag,
    String hash,
    List<StepType> order) {

  public WorkflowDefinition {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(domain, "domain");
    Objects.requireNonNull(dag, "dag");
    Objects.requireNonNull(hash, "hash");
    match = Map.copyOf(match);
    order = List.copyOf(order);
  }

  /** True when every match entry equals the corresponding command parameter. */
  public boolean matches(Domain commandDomain, Map<String, String> parameters) {
    if (domain != commandDomain) {
      return false;
    }
    return match.entrySet().stream()
        .allMatch(entry -> entry.getValue().equals(parameters.get(entry.getKey())));
  }

  /** Position of a step in {@link #order()}, starting at 1. */
  public int orderOf(StepType step) {
    int index = order.indexOf(step);
    if (index < 0) {
      throw new IllegalArgumentException("Step " + step + " is not part of workflow " + name);
    }
    return index + 1;
  }
}
