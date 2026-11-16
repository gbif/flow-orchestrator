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

import org.gbif.orchestrator.engine.DagDefinition;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Builds the configured workflow DAG at startup so that invalid definitions (cycles, unknown
 * dependencies, duplicate nodes) fail fast instead of on the first workflow event.
 */
@Configuration(proxyBeanMethods = false)
public class WorkflowConfiguration {

  private static final Logger LOG = LoggerFactory.getLogger(WorkflowConfiguration.class);

  @Bean
  public Optional<DagDefinition> dagDefinition(DagProperties properties) {
    if (properties.getNodes().isEmpty()) {
      LOG.info("No workflow DAG configured");
      return Optional.empty();
    }
    DagDefinition definition = properties.toDefinition();
    LOG.info(
        "Configured workflow DAG steps={} parallel={} defaultJoinPolicy={}",
        definition.stepTypes(),
        definition.parallel(),
        properties.getDefaultJoinPolicy());
    return Optional.of(definition);
  }
}
