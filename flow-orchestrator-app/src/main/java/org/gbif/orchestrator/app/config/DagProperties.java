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
import org.gbif.orchestrator.engine.DagDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Optional DAG definition, separate from the step-to-messaging routing configuration. */
@ConfigurationProperties(prefix = "orchestrator.workflow")
public class DagProperties {

  private boolean parallel = false;
  private DagDefinition.JoinPolicy defaultJoinPolicy = DagDefinition.JoinPolicy.NON_ERROR;
  private Map<StepType, NodeProperties> nodes = Map.of();

  public boolean isParallel() {
    return parallel;
  }

  public void setParallel(boolean parallel) {
    this.parallel = parallel;
  }

  public DagDefinition.JoinPolicy getDefaultJoinPolicy() {
    return defaultJoinPolicy;
  }

  public void setDefaultJoinPolicy(DagDefinition.JoinPolicy defaultJoinPolicy) {
    this.defaultJoinPolicy = defaultJoinPolicy;
  }

  public Map<StepType, NodeProperties> getNodes() {
    return nodes;
  }

  public void setNodes(Map<StepType, NodeProperties> nodes) {
    this.nodes = nodes;
  }

  public DagDefinition toDefinition() {
    List<DagDefinition.Node> configuredNodes = new ArrayList<>();

    for (Map.Entry<StepType, NodeProperties> entry : nodes.entrySet()) {
      NodeProperties value = entry.getValue();
      configuredNodes.add(
          new DagDefinition.Node(
              entry.getKey(), Set.copyOf(value.getDependencies()), value.getJoinPolicy()));
    }

    return new DagDefinition(configuredNodes, defaultJoinPolicy, parallel);
  }

  public static class NodeProperties {
    private List<StepType> dependencies = List.of();
    private DagDefinition.JoinPolicy joinPolicy;

    public List<StepType> getDependencies() {
      return dependencies;
    }

    public void setDependencies(List<StepType> dependencies) {
      this.dependencies = dependencies;
    }

    public DagDefinition.JoinPolicy getJoinPolicy() {
      return joinPolicy;
    }

    public void setJoinPolicy(DagDefinition.JoinPolicy joinPolicy) {
      this.joinPolicy = joinPolicy;
    }
  }
}
