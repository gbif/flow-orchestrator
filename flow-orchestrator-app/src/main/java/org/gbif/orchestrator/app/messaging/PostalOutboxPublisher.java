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
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.orchestrator.app.json.ContractJson;
import org.gbif.orchestrator.app.persistence.OutboxMessage;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Publishes outbox rows through a postal publisher built with publisher confirms and the mandatory
 * flag, so a send returns only after the broker confirmed it, and an unroutable message fails.
 *
 * <p>The stored payload is already JSON; it is sent as a {@link JsonNode} so postal does not
 * encode it a second time. The outbox {@code message_id} becomes the AMQP message id, for
 * deduplication by receivers.
 */
public class PostalOutboxPublisher implements OutboxPublisher {

  private final MessagePublisher publisher;
  private final ContractJson json;

  public PostalOutboxPublisher(MessagePublisher publisher, ContractJson json) {
    this.publisher = publisher;
    this.json = json;
  }

  @Override
  public void publish(OutboxMessage message) throws Exception {
    JsonNode body = json.read(message.payload(), JsonNode.class);
    publisher.send(
        body,
        message.exchange(),
        message.routingKey(),
        SendOptions.builder()
            .persistent(true)
            .messageId(message.messageId().toString())
            .contentType("application/json")
            .header("workflowId", message.workflowExecutionId().toString())
            .build());
  }
}
