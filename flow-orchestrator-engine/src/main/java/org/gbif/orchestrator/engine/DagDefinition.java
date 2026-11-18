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
package org.gbif.orchestrator.engine;

import org.gbif.api.model.pipelines.StepType;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Immutable workflow dependencies and optional per-node join policies. */
public final class DagDefinition {

  public enum JoinPolicy {
    NON_ERROR,
    ALL_TERMINAL
  }

  public record Node(StepType type, Set<StepType> dependencies, JoinPolicy joinPolicy) {
    public Node {
      Objects.requireNonNull(type, "type");
      dependencies = Set.copyOf(Objects.requireNonNull(dependencies, "dependencies"));
      if (dependencies.contains(type)) {
        throw new IllegalArgumentException("Self dependency: " + type);
      }
    }

    public Node(StepType type, Set<StepType> dependencies) {
      this(type, dependencies, null);
    }
  }

  private final Map<StepType, Node> nodes;
  private final JoinPolicy defaultJoinPolicy;
  private final boolean parallel;

  /**
   * A strictly sequential workflow: each step depends on the previous one, using the {@link
   * JoinPolicy#NON_ERROR} policy and no parallel dispatch.
   */
  public static DagDefinition linear(List<StepType> steps) {
    List<Node> nodes = new ArrayList<>();
    StepType previous = null;
    for (StepType step : steps) {
      nodes.add(new Node(step, previous == null ? Set.of() : Set.of(previous)));
      previous = step;
    }
    return new DagDefinition(nodes, JoinPolicy.NON_ERROR, false);
  }

  public DagDefinition(List<Node> nodes, JoinPolicy defaultJoinPolicy, boolean parallel) {
    this.defaultJoinPolicy = Objects.requireNonNull(defaultJoinPolicy, "defaultJoinPolicy");
    this.parallel = parallel;

    Map<StepType, Node> byType = new EnumMap<>(StepType.class);
    for (Node node : nodes) {
      if (byType.putIfAbsent(node.type(), node) != null) {
        throw new IllegalArgumentException("Duplicate node: " + node.type());
      }
    }

    for (Node node : nodes) {
      for (StepType predecessor : node.dependencies()) {
        if (!byType.containsKey(predecessor)) {
          throw new IllegalArgumentException(
              "Unknown dependency " + predecessor + " for " + node.type());
        }
      }
    }

    this.nodes = Map.copyOf(byType);
    Map<StepType, Boolean> visited = new HashMap<>();
    for (StepType type : byType.keySet()) {
      checkAcyclic(type, visited);
    }
  }

  private void checkAcyclic(StepType type, Map<StepType, Boolean> visited) {
    Boolean state = visited.get(type);
    if (Boolean.TRUE.equals(state)) {
      return;
    }
    if (Boolean.FALSE.equals(state)) {
      throw new IllegalArgumentException("Cycle involving " + type);
    }

    visited.put(type, false);
    for (StepType dependency : nodes.get(type).dependencies()) {
      checkAcyclic(dependency, visited);
    }
    visited.put(type, true);
  }

  public Node require(StepType type) {
    Node node = nodes.get(type);
    if (node == null) {
      throw new IllegalArgumentException("Unknown DAG node " + type);
    }
    return node;
  }

  public Set<StepType> stepTypes() {
    return nodes.keySet();
  }

  /** The steps that depend on {@code type}, directly or transitively; {@code type} excluded. */
  public Set<StepType> downstreamOf(StepType type) {
    require(type);
    Set<StepType> downstream = new LinkedHashSet<>();
    Deque<StepType> pending = new ArrayDeque<>(List.of(type));
    while (!pending.isEmpty()) {
      StepType current = pending.pop();
      for (Node node : nodes.values()) {
        if (node.dependencies().contains(current) && downstream.add(node.type())) {
          pending.push(node.type());
        }
      }
    }
    return downstream;
  }

  public JoinPolicy policy(Node node) {
    return node.joinPolicy() == null ? defaultJoinPolicy : node.joinPolicy();
  }

  public boolean parallel() {
    return parallel;
  }

  public JoinPolicy defaultJoinPolicy() {
    return defaultJoinPolicy;
  }

  /**
   * All steps in a deterministic topological order: every step comes after its dependencies, and
   * steps that are ready at the same time are ordered by name.
   */
  public List<StepType> topologicalOrder() {
    Map<StepType, Integer> remaining = new HashMap<>();
    for (Node node : nodes.values()) {
      remaining.put(node.type(), node.dependencies().size());
    }
    java.util.TreeSet<StepType> ready =
        new java.util.TreeSet<>(java.util.Comparator.comparing(StepType::name));
    remaining.forEach(
        (type, count) -> {
          if (count == 0) {
            ready.add(type);
          }
        });
    List<StepType> order = new ArrayList<>(nodes.size());
    while (!ready.isEmpty()) {
      StepType next = ready.pollFirst();
      order.add(next);
      for (Node node : nodes.values()) {
        if (node.dependencies().contains(next) && remaining.merge(node.type(), -1, Integer::sum) == 0) {
          ready.add(node.type());
        }
      }
    }
    return List.copyOf(order);
  }
}
