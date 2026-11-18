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

import java.time.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Core orchestrator beans. */
@Configuration(proxyBeanMethods = false)
public class OrchestratorConfiguration {

  private static final Logger LOG = LoggerFactory.getLogger(OrchestratorConfiguration.class);

  @Bean
  public WorkflowDefinitions workflowDefinitions(OrchestratorProperties properties) {
    if (properties.getDomain() == null) {
      throw new IllegalStateException("orchestrator.domain must be configured");
    }
    // every terminal workflow writes this event, with or without a broker connection
    if (!properties.getMessaging().getWorkflowFinished().isComplete()) {
      throw new IllegalStateException(
          "orchestrator.messaging.workflow-finished needs exchange and routing-key: "
              + properties.getMessaging().getWorkflowFinished());
    }
    WorkflowDefinitions definitions = new WorkflowDefinitions(properties);
    for (WorkflowDefinition definition : definitions.all()) {
      LOG.info(
          "Configured workflow name={} domain={} match={} steps={} hash={}",
          definition.name(),
          definition.domain(),
          definition.match(),
          definition.order(),
          definition.hash());
    }
    LOG.info(
        "Orchestrator domain={} workflows={}", properties.getDomain(), definitions.all().size());
    return definitions;
  }

  @Bean
  public Clock clock() {
    return Clock.systemUTC();
  }
}
