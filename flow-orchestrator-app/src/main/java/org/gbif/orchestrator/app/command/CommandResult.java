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
package org.gbif.orchestrator.app.command;

import java.util.UUID;

/**
 * What happened to a command.
 *
 * @param workflowExecutionId the execution created or affected; null unless accepted
 * @param reason why the command was rejected; null unless rejected
 */
public record CommandResult(Kind kind, UUID workflowExecutionId, String reason) {

  public enum Kind {
    ACCEPTED,
    REJECTED,
    /** The command id was already processed; nothing was done. */
    DUPLICATE
  }

  static CommandResult accepted(UUID workflowExecutionId) {
    return new CommandResult(Kind.ACCEPTED, workflowExecutionId, null);
  }

  static CommandResult rejected(String reason) {
    return new CommandResult(Kind.REJECTED, null, reason);
  }

  static CommandResult duplicate() {
    return new CommandResult(Kind.DUPLICATE, null, null);
  }
}
