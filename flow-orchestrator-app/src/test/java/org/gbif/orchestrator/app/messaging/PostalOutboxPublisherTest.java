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

import org.gbif.common.messaging.SendOptions;
import org.gbif.common.messaging.api.Message;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.orchestrator.app.json.ContractJson;
import org.gbif.orchestrator.app.persistence.OutboxMessage;

import java.time.Instant;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PostalOutboxPublisherTest {

  @Test
  void sendsThePayloadAsJsonWithTheMessageId() throws Exception {
    RecordingPublisher postal = new RecordingPublisher();
    ContractJson json = new ContractJson();
    UUID messageId = UUID.randomUUID();
    UUID workflowId = UUID.randomUUID();
    OutboxMessage message =
        new OutboxMessage(
            1L,
            messageId,
            workflowId,
            null,
            null,
            "occurrence",
            "validator.dwcdp.analysis",
            "{\"header\":{\"stepAttempt\":1}}",
            Instant.now(),
            null,
            0,
            null,
            null);

    new PostalOutboxPublisher(postal, json).publish(message);

    assertEquals("occurrence", postal.exchange);
    assertEquals("validator.dwcdp.analysis", postal.routingKey);
    // a JSON tree, so postal does not encode the stored JSON text a second time
    assertEquals(json.read(message.payload(), JsonNode.class), postal.message);
    assertEquals(messageId.toString(), postal.options.getMessageId());
    assertEquals(true, postal.options.isPersistent());
    assertEquals("application/json", postal.options.getContentType());
    assertEquals(workflowId.toString(), postal.options.getHeaders().get("workflowId"));
  }

  /** Captures the SendOptions send; the other methods are not used by the relay. */
  static class RecordingPublisher implements MessagePublisher {
    Object message;
    String exchange;
    String routingKey;
    SendOptions options;

    @Override
    public void send(Object message, String exchange, String routingKey, SendOptions options) {
      this.message = message;
      this.exchange = exchange;
      this.routingKey = routingKey;
      this.options = options;
    }

    @Override
    public void send(Message message) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void send(Message message, boolean persistent) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void send(Message message, String exchange) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void send(Object message, String exchange, String routingKey) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void send(Object message, String exchange, String routingKey, boolean persistent) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void replyToQueue(
        Object message, boolean persistent, String correlationId, String replyTo) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T sendAndReceive(
        Message message, String routingKey, boolean persistent, String correlationId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T sendAndReceive(
        Object message,
        String exchange,
        String routingKey,
        boolean persistent,
        String correlationId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {}
  }
}
