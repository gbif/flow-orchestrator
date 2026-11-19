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
package org.gbif.orchestrator.ws;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Settings of flow-orchestrator-ws that are new with the orchestrator. The settings carried over
 * from validator-ws keep their names (for example {@code maxRunningValidationPerUser}, {@code
 * storePath}, {@code clb.*}, {@code retentionPolicy.*}).
 */
@ConfigurationProperties(prefix = "orchestrator.ws")
public class WsProperties {

  private Rabbit rabbitmq = new Rabbit();
  private Messaging messaging = new Messaging();

  public Rabbit getRabbitmq() {
    return rabbitmq;
  }

  public void setRabbitmq(Rabbit rabbitmq) {
    this.rabbitmq = rabbitmq;
  }

  public Messaging getMessaging() {
    return messaging;
  }

  public void setMessaging(Messaging messaging) {
    this.messaging = messaging;
  }

  public static class Rabbit {
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

  public static class Messaging {
    /** Off in tests: no broker, the services are called directly. */
    private boolean enabled = true;

    private String deadLetterExchange = "orchestrator.dlx";
    /** Where the engine (flow-orchestrator-app, validation domain) takes commands. */
    private Route commands = new Route();
    /** The engine's WorkflowFinished events; this service binds its own queue. */
    private Route workflowFinished = new Route();
    /** Queue for {@link #workflowFinished}. */
    private String workflowFinishedQueue;

    private int workflowFinishedThreads = 1;
    /** How long a publish waits for the broker's confirm. */
    private java.time.Duration confirmTimeout = java.time.Duration.ofSeconds(10);

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

    public Route getCommands() {
      return commands;
    }

    public void setCommands(Route commands) {
      this.commands = commands;
    }

    public Route getWorkflowFinished() {
      return workflowFinished;
    }

    public void setWorkflowFinished(Route workflowFinished) {
      this.workflowFinished = workflowFinished;
    }

    public String getWorkflowFinishedQueue() {
      return workflowFinishedQueue;
    }

    public void setWorkflowFinishedQueue(String workflowFinishedQueue) {
      this.workflowFinishedQueue = workflowFinishedQueue;
    }

    public int getWorkflowFinishedThreads() {
      return workflowFinishedThreads;
    }

    public void setWorkflowFinishedThreads(int workflowFinishedThreads) {
      this.workflowFinishedThreads = workflowFinishedThreads;
    }

    public java.time.Duration getConfirmTimeout() {
      return confirmTimeout;
    }

    public void setConfirmTimeout(java.time.Duration confirmTimeout) {
      this.confirmTimeout = confirmTimeout;
    }
  }

  /** An exchange and routing key. */
  public static class Route {
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

    @Override
    public String toString() {
      return exchange + "/" + routingKey;
    }
  }
}
