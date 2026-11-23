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
package org.gbif.orchestrator.ws.messaging;

import org.gbif.common.messaging.ConnectionParameters;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.QueueBinding;
import org.gbif.orchestrator.api.json.OrchestratorJson;
import org.gbif.orchestrator.ws.WsProperties;
import org.gbif.orchestrator.ws.service.ValidationServiceImpl;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * RabbitMQ through postal: one confirming publisher for engine commands and pipelines messages,
 * and the listeners for WorkflowFinished and the ChecklistBank step requests. Off with {@code
 * orchestrator.ws.messaging.enabled=false} (tests provide {@link Messages}).
 */
@Configuration
@ConditionalOnProperty(
    prefix = "orchestrator.ws.messaging",
    name = "enabled",
    havingValue = "true",
    matchIfMissing = true)
public class MessagingConfiguration {

  private static final Logger LOG = LoggerFactory.getLogger(MessagingConfiguration.class);

  @Bean
  public ConnectionParameters connectionParameters(WsProperties properties) {
    validate(properties.getMessaging());
    WsProperties.Rabbit rabbit = properties.getRabbitmq();
    return new ConnectionParameters(
        rabbit.getHost(),
        rabbit.getPort(),
        rabbit.getUsername(),
        rabbit.getPassword(),
        rabbit.getVirtualHost());
  }

  /** Waits for broker confirms and fails unroutable messages, like the engine's publisher. */
  @Bean(destroyMethod = "close")
  public DefaultMessagePublisher messagePublisher(
      ConnectionParameters connection, WsProperties properties) throws IOException {
    return DefaultMessagePublisher.builder(connection)
        .mapper(OrchestratorJson.mapper())
        .declareRegisteredExchanges(false)
        .declareExchangeOnSend(true)
        .publisherConfirms(properties.getMessaging().getConfirmTimeout())
        .mandatory(true)
        .build();
  }

  @Bean
  public Messages messages(DefaultMessagePublisher publisher, WsProperties properties) {
    return new PostalMessages(publisher, properties.getMessaging().getCommands());
  }

  @Bean
  public MessageListener messageListener(ConnectionParameters connection) throws IOException {
    // a fresh mapper: postal reconfigures the one it is given
    return MessageListener.builder(connection).mapper(OrchestratorJson.mapper()).build();
  }

  /** Starts listening once the application context is ready, stops on shutdown. */
  @Bean
  public SmartLifecycle workflowFinishedListening(
      MessageListener listener, WsProperties properties, ValidationServiceImpl validations) {
    WsProperties.Messaging messaging = properties.getMessaging();
    return new SmartLifecycle() {
      private volatile boolean running;

      @Override
      public synchronized void start() {
        QueueBinding binding =
            QueueBinding.builder(
                    messaging.getWorkflowFinishedQueue(),
                    messaging.getWorkflowFinished().getExchange(),
                    messaging.getWorkflowFinished().getRoutingKey())
                .threads(messaging.getWorkflowFinishedThreads())
                .deadLetter(messaging.getDeadLetterExchange())
                .build();
        QueueBinding checklistbank =
            QueueBinding.builder(
                    messaging.getChecklistbankQueue(),
                    messaging.getChecklistbank().getExchange(),
                    messaging.getChecklistbank().getRoutingKey())
                .deadLetter(messaging.getDeadLetterExchange())
                .build();
        try {
          listener.listen(binding, new WorkflowFinishedListener(validations));
          listener.listen(checklistbank, new ChecklistbankRequestListener(validations));
        } catch (IOException e) {
          throw new UncheckedIOException("Cannot listen on " + binding + " / " + checklistbank, e);
        }
        running = true;
        LOG.info("Listening {} for WorkflowFinished", binding);
        LOG.info("Listening {} for ChecklistBank step requests", checklistbank);
      }

      @Override
      public synchronized void stop() {
        if (running) {
          listener.close();
          running = false;
        }
      }

      @Override
      public boolean isRunning() {
        return running;
      }
    };
  }

  static void validate(WsProperties.Messaging messaging) {
    if (!complete(messaging.getCommands())) {
      throw new IllegalStateException(
          "orchestrator.ws.messaging.commands needs exchange and routing-key");
    }
    if (!complete(messaging.getWorkflowFinished())
        || isBlank(messaging.getWorkflowFinishedQueue())) {
      throw new IllegalStateException(
          "orchestrator.ws.messaging.workflow-finished needs exchange and routing-key, and "
              + "orchestrator.ws.messaging.workflow-finished-queue a name");
    }
    if (!complete(messaging.getChecklistbank()) || isBlank(messaging.getChecklistbankQueue())) {
      throw new IllegalStateException(
          "orchestrator.ws.messaging.checklistbank needs exchange and routing-key, and "
              + "orchestrator.ws.messaging.checklistbank-queue a name");
    }
  }

  private static boolean complete(WsProperties.Route route) {
    return !isBlank(route.getExchange()) && !isBlank(route.getRoutingKey());
  }

  private static boolean isBlank(String value) {
    return value == null || value.isBlank();
  }
}
