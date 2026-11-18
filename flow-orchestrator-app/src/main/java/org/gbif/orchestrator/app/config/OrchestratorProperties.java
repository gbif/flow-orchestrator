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

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Orchestrator configuration: the instance's domain, its workflow definitions, and where each step
 * request is sent.
 *
 * <pre>
 * orchestrator:
 *   domain: VALIDATION
 *   workflows:
 *     validation-dwcdp:
 *       domain: VALIDATION
 *       match: { fileFormat: DWCDP }
 *       nodes:
 *         - step: VALIDATOR_VALIDATE_DWCDP
 *         - step: VALIDATOR_DWCDP_STAGE
 *           dependencies: [VALIDATOR_VALIDATE_DWCDP]
 *   steps:
 *     VALIDATOR_VALIDATE_DWCDP:
 *       request: { exchange: occurrence, routing-key: validator.dwcdp.analysis }
 *   messaging:
 *     dead-letter-exchange: orchestrator.dlx
 *     commands: { exchange: orchestrator, routing-key: validation.command, queue: validation.commands }
 *     completions: { exchange: orchestrator, routing-key: validation.step.completed, queue: ... }
 *     workflow-finished: { exchange: orchestrator, routing-key: validation.workflow.finished }
 * </pre>
 */
@ConfigurationProperties(prefix = "orchestrator")
public class OrchestratorProperties {

  /** The single domain this instance processes. Required. */
  private Domain domain;

  private Map<String, Workflow> workflows = new LinkedHashMap<>();

  private Map<StepType, Step> steps = new LinkedHashMap<>();

  private Rabbitmq rabbitmq = new Rabbitmq();

  private Messaging messaging = new Messaging();

  private Relay relay = new Relay();

  public Domain getDomain() {
    return domain;
  }

  public void setDomain(Domain domain) {
    this.domain = domain;
  }

  public Map<String, Workflow> getWorkflows() {
    return workflows;
  }

  public void setWorkflows(Map<String, Workflow> workflows) {
    this.workflows = workflows;
  }

  public Map<StepType, Step> getSteps() {
    return steps;
  }

  public void setSteps(Map<StepType, Step> steps) {
    this.steps = steps;
  }

  public Rabbitmq getRabbitmq() {
    return rabbitmq;
  }

  public void setRabbitmq(Rabbitmq rabbitmq) {
    this.rabbitmq = rabbitmq;
  }

  public Messaging getMessaging() {
    return messaging;
  }

  public void setMessaging(Messaging messaging) {
    this.messaging = messaging;
  }

  public Relay getRelay() {
    return relay;
  }

  public void setRelay(Relay relay) {
    this.relay = relay;
  }

  /** One workflow definition, selected by domain and command parameters. */
  public static class Workflow {
    private Domain domain;
    private Map<String, String> match = new LinkedHashMap<>();
    private boolean parallel = false;
    private DagDefinition.JoinPolicy defaultJoinPolicy = DagDefinition.JoinPolicy.NON_ERROR;
    private List<Node> nodes = new ArrayList<>();

    public Domain getDomain() {
      return domain;
    }

    public void setDomain(Domain domain) {
      this.domain = domain;
    }

    public Map<String, String> getMatch() {
      return match;
    }

    public void setMatch(Map<String, String> match) {
      this.match = match;
    }

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

    public List<Node> getNodes() {
      return nodes;
    }

    public void setNodes(List<Node> nodes) {
      this.nodes = nodes;
    }
  }

  /** A DAG node: its dependencies and an optional join policy override. */
  public static class Node {
    private StepType step;
    private List<StepType> dependencies = List.of();
    private DagDefinition.JoinPolicy joinPolicy;

    public StepType getStep() {
      return step;
    }

    public void setStep(StepType step) {
      this.step = step;
    }

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

  /** Per-step transport settings. */
  public static class Step {
    private Destination request = new Destination();

    public Destination getRequest() {
      return request;
    }

    public void setRequest(Destination request) {
      this.request = request;
    }
  }

  /** An exchange and routing key. */
  public static class Destination {
    private String exchange;
    private String routingKey;

    public String getExchange() {
      return exchange;
    }

    public void setExchange(String exchange) {
      this.exchange = exchange;
    }

    public String getRoutingKey() {
      return routingKey;
    }

    public void setRoutingKey(String routingKey) {
      this.routingKey = routingKey;
    }

    public boolean isComplete() {
      return isName(exchange) && isName(routingKey);
    }

    @Override
    public String toString() {
      return "exchange=" + exchange + ", routingKey=" + routingKey;
    }
  }

  /** A queue to consume: declared durable, bound to the exchange with the routing key. */
  public static class Listener extends Destination {
    private String queue;
    private int threads = 1;

    public String getQueue() {
      return queue;
    }

    public void setQueue(String queue) {
      this.queue = queue;
    }

    public int getThreads() {
      return threads;
    }

    public void setThreads(int threads) {
      this.threads = threads;
    }

    @Override
    public boolean isComplete() {
      return super.isComplete() && isName(queue) && threads >= 1;
    }

    /** Nothing configured, for optional listeners. */
    public boolean isUnset() {
      return isBlank(queue) && isBlank(getExchange()) && isBlank(getRoutingKey());
    }

    private static boolean isBlank(String value) {
      return value == null || value.isBlank();
    }

    @Override
    public String toString() {
      return "queue=" + queue + ", " + super.toString() + ", threads=" + threads;
    }
  }

  /** RabbitMQ connection. */
  public static class Rabbitmq {
    private String host = "localhost";
    private int port = 5672;
    private String username = "guest";
    private String password = "guest";
    private String virtualHost = "/";

    public String getHost() {
      return host;
    }

    public void setHost(String host) {
      this.host = host;
    }

    public int getPort() {
      return port;
    }

    public void setPort(int port) {
      this.port = port;
    }

    public String getUsername() {
      return username;
    }

    public void setUsername(String username) {
      this.username = username;
    }

    public String getPassword() {
      return password;
    }

    public void setPassword(String password) {
      this.password = password;
    }

    public String getVirtualHost() {
      return virtualHost;
    }

    public void setVirtualHost(String virtualHost) {
      this.virtualHost = virtualHost;
    }
  }

  /**
   * Queues the orchestrator consumes and the event it publishes. Dead-lettered messages go to
   * {@code <queue>.dlq} through {@code deadLetterExchange}.
   */
  public static class Messaging {
    /** Whether to connect to RabbitMQ (listeners and outbox relay). Off in tests. */
    private boolean enabled = true;

    private String deadLetterExchange;
    private Listener commands = new Listener();
    private Listener completions = new Listener();

    /**
     * Legacy completions of the dwc-dp-analyser-service ({@code DwcDpValidationFinished}), mapped
     * to step completions. Optional: not listened to when no queue is configured.
     */
    private Listener dwcdpAnalysisFinished = new Listener();

    private Destination workflowFinished = new Destination();

    public boolean isEnabled() {
      return enabled;
    }

    public void setEnabled(boolean enabled) {
      this.enabled = enabled;
    }

    public String getDeadLetterExchange() {
      return deadLetterExchange;
    }

    public void setDeadLetterExchange(String deadLetterExchange) {
      this.deadLetterExchange = deadLetterExchange;
    }

    public Listener getCommands() {
      return commands;
    }

    public void setCommands(Listener commands) {
      this.commands = commands;
    }

    public Listener getCompletions() {
      return completions;
    }

    public void setCompletions(Listener completions) {
      this.completions = completions;
    }

    public Listener getDwcdpAnalysisFinished() {
      return dwcdpAnalysisFinished;
    }

    public void setDwcdpAnalysisFinished(Listener dwcdpAnalysisFinished) {
      this.dwcdpAnalysisFinished = dwcdpAnalysisFinished;
    }

    public Destination getWorkflowFinished() {
      return workflowFinished;
    }

    public void setWorkflowFinished(Destination workflowFinished) {
      this.workflowFinished = workflowFinished;
    }
  }

  /** Outbox relay timing. */
  public static class Relay {
    private Duration pollInterval = Duration.ofSeconds(1);
    private int batchSize = 100;
    private Duration confirmTimeout = Duration.ofSeconds(10);
    private Duration maxBackoff = Duration.ofMinutes(5);

    public Duration getPollInterval() {
      return pollInterval;
    }

    public void setPollInterval(Duration pollInterval) {
      this.pollInterval = pollInterval;
    }

    public int getBatchSize() {
      return batchSize;
    }

    public void setBatchSize(int batchSize) {
      this.batchSize = batchSize;
    }

    public Duration getConfirmTimeout() {
      return confirmTimeout;
    }

    public void setConfirmTimeout(Duration confirmTimeout) {
      this.confirmTimeout = confirmTimeout;
    }

    public Duration getMaxBackoff() {
      return maxBackoff;
    }

    public void setMaxBackoff(Duration maxBackoff) {
      this.maxBackoff = maxBackoff;
    }
  }

  /**
   * A usable AMQP name: not blank, and no empty dot-separated part (which an empty prefix such as
   * {@code orchestrator..commands} would give).
   */
  public static boolean isName(String name) {
    return name != null
        && !name.isBlank()
        && !name.startsWith(".")
        && !name.endsWith(".")
        && !name.contains("..");
  }
}
