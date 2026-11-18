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

import org.gbif.common.messaging.api.MessageCallback;
import org.gbif.orchestrator.api.command.WorkflowCommand;
import org.gbif.orchestrator.app.command.CommandService;

/**
 * Consumes workflow commands. Accepted, rejected and duplicate commands are all acknowledged (the
 * outcome is stored on the command row); an exception, for example a conflict that persists after
 * the retries, dead-letters the message.
 */
public class CommandListener implements MessageCallback<WorkflowCommand> {

  private final CommandService commands;

  public CommandListener(CommandService commands) {
    this.commands = commands;
  }

  @Override
  public void handleMessage(WorkflowCommand command) {
    commands.handle(command);
  }

  @Override
  public Class<WorkflowCommand> getMessageClass() {
    return WorkflowCommand.class;
  }
}
