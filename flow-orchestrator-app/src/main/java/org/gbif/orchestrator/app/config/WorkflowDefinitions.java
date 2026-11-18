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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * The configured workflow definitions, validated at startup. Invalid configuration (cyclic or
 * dangling DAGs, ambiguous matches, steps without request routing) prevents the application from
 * starting.
 */
public final class WorkflowDefinitions {

  private final List<WorkflowDefinition> definitions;

  public WorkflowDefinitions(OrchestratorProperties properties) {
    List<WorkflowDefinition> built = new ArrayList<>();
    for (Map.Entry<String, OrchestratorProperties.Workflow> entry :
        properties.getWorkflows().entrySet()) {
      built.add(build(entry.getKey(), entry.getValue(), properties.getSteps()));
    }
    for (int i = 0; i < built.size(); i++) {
      for (int j = i + 1; j < built.size(); j++) {
        WorkflowDefinition a = built.get(i);
        WorkflowDefinition b = built.get(j);
        if (a.domain() == b.domain() && a.match().equals(b.match())) {
          throw new IllegalStateException(
              "Workflows " + a.name() + " and " + b.name() + " have the same domain and match");
        }
      }
    }
    this.definitions = List.copyOf(built);
  }

  /**
   * The definition selected by a command's domain and parameters. Empty when none or more than one
   * match; a command must select exactly one workflow.
   */
  public Optional<WorkflowDefinition> resolve(Domain domain, Map<String, String> parameters) {
    List<WorkflowDefinition> matches =
        definitions.stream().filter(d -> d.matches(domain, parameters)).toList();
    return matches.size() == 1 ? Optional.of(matches.get(0)) : Optional.empty();
  }

  /** The definition with the given name. */
  public Optional<WorkflowDefinition> byName(String name) {
    return definitions.stream().filter(d -> d.name().equals(name)).findFirst();
  }

  public List<WorkflowDefinition> all() {
    return definitions;
  }

  private static WorkflowDefinition build(
      String name,
      OrchestratorProperties.Workflow workflow,
      Map<StepType, OrchestratorProperties.Step> steps) {
    if (workflow.getDomain() == null) {
      throw new IllegalStateException("Workflow " + name + " has no domain");
    }
    if (workflow.getNodes().isEmpty()) {
      throw new IllegalStateException("Workflow " + name + " has no nodes");
    }
    List<DagDefinition.Node> nodes = new ArrayList<>();
    for (OrchestratorProperties.Node node : workflow.getNodes()) {
      if (node.getStep() == null) {
        throw new IllegalStateException("Workflow " + name + " has a node without step");
      }
      nodes.add(
          new DagDefinition.Node(
              node.getStep(), Set.copyOf(node.getDependencies()), node.getJoinPolicy()));
    }
    DagDefinition dag;
    try {
      dag = new DagDefinition(nodes, workflow.getDefaultJoinPolicy(), workflow.isParallel());
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException("Invalid workflow " + name + ": " + e.getMessage(), e);
    }
    for (StepType step : dag.stepTypes()) {
      OrchestratorProperties.Step routing = steps.get(step);
      if (routing == null || routing.getRequest() == null || !routing.getRequest().isComplete()) {
        throw new IllegalStateException(
            "Workflow " + name + " uses step " + step + " without request routing");
      }
    }
    return new WorkflowDefinition(
        name,
        workflow.getDomain(),
        workflow.getMatch(),
        dag,
        hash(workflow.getDomain(), workflow.getMatch(), dag),
        dag.topologicalOrder());
  }

  /** SHA-256 over a canonical, order-independent text form of the definition. */
  static String hash(Domain domain, Map<String, String> match, DagDefinition dag) {
    StringBuilder canonical = new StringBuilder();
    canonical.append("domain=").append(domain).append('\n');
    canonical.append("match=").append(new TreeMap<>(match)).append('\n');
    canonical.append("parallel=").append(dag.parallel()).append('\n');
    canonical.append("defaultJoinPolicy=").append(dag.defaultJoinPolicy()).append('\n');
    dag.stepTypes().stream()
        .sorted()
        .forEach(
            step -> {
              DagDefinition.Node node = dag.require(step);
              canonical
                  .append(step)
                  .append(" <- ")
                  .append(
                      node.dependencies().stream()
                          .map(Enum::name)
                          .sorted()
                          .collect(Collectors.joining(",")))
                  .append(" join=")
                  .append(node.joinPolicy())
                  .append('\n');
            });
    try {
      byte[] digest =
          MessageDigest.getInstance("SHA-256")
              .digest(canonical.toString().getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(digest);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }
}
