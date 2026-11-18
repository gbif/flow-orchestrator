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
package org.gbif.orchestrator.app.messaging;

import org.gbif.common.messaging.ConnectionParameters;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.orchestrator.api.json.OrchestratorJson;
import org.gbif.orchestrator.app.command.CommandService;
import org.gbif.orchestrator.app.completion.CompletionService;
import org.gbif.orchestrator.app.completion.DwcDpAnalysisAdapter;
import org.gbif.orchestrator.app.config.OrchestratorProperties;
import org.gbif.orchestrator.app.json.ContractJson;
import org.gbif.orchestrator.app.persistence.OutboxRepository;

import java.io.IOException;
import java.time.Clock;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * RabbitMQ through postal: the confirming publisher and outbox relay, and the command and
 * completion listeners. Off with {@code orchestrator.messaging.enabled=false} (tests).
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(
    prefix = "orchestrator.messaging",
    name = "enabled",
    havingValue = "true",
    matchIfMissing = true)
public class MessagingConfiguration {

  @Bean
  public ConnectionParameters connectionParameters(OrchestratorProperties properties) {
    validate(properties.getMessaging());
    OrchestratorProperties.Rabbitmq rabbit = properties.getRabbitmq();
    return new ConnectionParameters(
        rabbit.getHost(),
        rabbit.getPort(),
        rabbit.getUsername(),
        rabbit.getPassword(),
        rabbit.getVirtualHost());
  }

  /** Waits for broker confirms and fails unroutable messages; declares target exchanges. */
  @Bean(destroyMethod = "close")
  public DefaultMessagePublisher messagePublisher(
      ConnectionParameters connection, OrchestratorProperties properties) throws IOException {
    return DefaultMessagePublisher.builder(connection)
        .mapper(OrchestratorJson.mapper())
        .declareRegisteredExchanges(false)
        .declareExchangeOnSend(true)
        .publisherConfirms(properties.getRelay().getConfirmTimeout())
        .mandatory(true)
        .build();
  }

  @Bean
  public OutboxRelay outboxRelay(
      OutboxRepository outbox,
      DefaultMessagePublisher publisher,
      ContractJson json,
      OrchestratorProperties properties,
      Clock clock,
      MeterRegistry meters) {
    return new OutboxRelay(
        outbox, new PostalOutboxPublisher(publisher, json), properties, clock, meters);
  }

  @Bean
  public MessageListener messageListener(ConnectionParameters connection) throws IOException {
    // a fresh mapper: postal reconfigures the one it is given
    return MessageListener.builder(connection).mapper(OrchestratorJson.mapper()).build();
  }

  @Bean
  public ListenerLifecycle listenerLifecycle(
      MessageListener listener,
      OrchestratorProperties properties,
      CommandService commands,
      CompletionService completions,
      DwcDpAnalysisAdapter analysis) {
    return new ListenerLifecycle(
        listener,
        properties.getMessaging(),
        new CommandListener(commands),
        new CompletionListener(completions),
        new DwcDpAnalysisListener(analysis));
  }

  static void validate(OrchestratorProperties.Messaging messaging) {
    OrchestratorProperties.Listener analysis = messaging.getDwcdpAnalysisFinished();
    if (!analysis.isUnset() && !analysis.isComplete()) {
      throw new IllegalStateException(
          "orchestrator.messaging.dwcdp-analysis-finished is optional, but when set needs exchange, "
              + "routing-key, queue and threads >= 1: "
              + analysis);
    }
    if (!OrchestratorProperties.isName(messaging.getDeadLetterExchange())) {
      throw new IllegalStateException(
          "orchestrator.messaging.dead-letter-exchange must be configured");
    }
    if (!messaging.getCommands().isComplete()) {
      throw new IllegalStateException(
          "orchestrator.messaging.commands needs exchange, routing-key, queue and threads >= 1: "
              + messaging.getCommands());
    }
    if (!messaging.getCompletions().isComplete()) {
      throw new IllegalStateException(
          "orchestrator.messaging.completions needs exchange, routing-key, queue and threads >= 1: "
              + messaging.getCompletions());
    }
  }
}
