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

import org.gbif.common.messaging.SendOptions;
import org.gbif.common.messaging.api.Message;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.orchestrator.api.command.WorkflowCommand;
import org.gbif.orchestrator.ws.WsProperties;

import java.io.IOException;

/** {@link Messages} through postal, with publisher confirms and mandatory routing. */
public class PostalMessages implements Messages {

  private final MessagePublisher publisher;
  private final WsProperties.Route commands;

  public PostalMessages(MessagePublisher publisher, WsProperties.Route commands) {
    this.publisher = publisher;
    this.commands = commands;
  }

  @Override
  public void send(WorkflowCommand command) throws IOException {
    publisher.send(
        command,
        commands.getExchange(),
        commands.getRoutingKey(),
        SendOptions.builder()
            .persistent(true)
            .messageId(command.commandId().toString())
            .contentType("application/json")
            .build());
  }

  @Override
  public void send(Message message) throws IOException {
    publisher.send(message);
  }
}
