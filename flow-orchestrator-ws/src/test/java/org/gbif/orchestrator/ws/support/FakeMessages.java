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
package org.gbif.orchestrator.ws.support;

import org.gbif.common.messaging.api.Message;
import org.gbif.orchestrator.api.command.WorkflowCommand;
import org.gbif.orchestrator.ws.messaging.Messages;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** Records what would be sent; can be told to fail. */
public class FakeMessages implements Messages {

  public final List<WorkflowCommand> commands = new CopyOnWriteArrayList<>();
  public final List<Message> legacy = new CopyOnWriteArrayList<>();
  public volatile boolean fail;

  @Override
  public void send(WorkflowCommand command) throws IOException {
    if (fail) {
      throw new IOException("broker down");
    }
    commands.add(command);
  }

  @Override
  public void send(Message message) throws IOException {
    if (fail) {
      throw new IOException("broker down");
    }
    legacy.add(message);
  }

  public void clear() {
    commands.clear();
    legacy.clear();
    fail = false;
  }
}
