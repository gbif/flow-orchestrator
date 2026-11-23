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
package org.gbif.orchestrator.ws.service;

import org.gbif.orchestrator.api.command.CommandType;
import org.gbif.orchestrator.api.command.Domain;
import org.gbif.orchestrator.api.command.WorkflowCommand;
import org.gbif.orchestrator.ws.api.FileFormat;
import org.gbif.orchestrator.ws.messaging.Messages;

import java.io.IOException;
import java.time.Clock;
import java.util.Map;
import java.util.UUID;

import org.springframework.stereotype.Service;

/**
 * Starts the work of a validation with a {@code WorkflowCommand}. The engine selects the workflow
 * by the parameters: {@code fileFormat}, and for DwC-A {@code checklist} (a Taxon core, which
 * waits for ChecklistBank). Every format the service accepts runs through the engine (Round B).
 */
@Service
public class ValidationMessageService {

  /** The engine's workflows are matched on this parameter (see the engine's workflow config). */
  public static final String FILE_FORMAT_PARAMETER = "fileFormat";

  /** {@code true} or {@code false} for DwC-A: whether the core is Taxon. */
  public static final String CHECKLIST_PARAMETER = "checklist";

  private final Messages messages;
  private final Clock clock;

  public ValidationMessageService(Messages messages, Clock clock) {
    this.messages = messages;
    this.clock = clock;
  }

  /**
   * True for formats whose validation runs as an engine workflow: since Round B every format the
   * service detects. Validations created before Round B have no workflow and keep their stored
   * state.
   */
  public static boolean isOrchestrated(FileFormat fileFormat) {
    return fileFormat != null;
  }

  /** The START parameters for an upload: the format, and for DwC-A whether it is a checklist. */
  static Map<String, String> parameters(FileFormat fileFormat, boolean checklist) {
    return fileFormat == FileFormat.DWCA
        ? Map.of(FILE_FORMAT_PARAMETER, fileFormat.name(), CHECKLIST_PARAMETER, String.valueOf(checklist))
        : Map.of(FILE_FORMAT_PARAMETER, fileFormat.name());
  }

  public void start(UUID validationKey, FileFormat fileFormat, boolean checklist, String user)
      throws IOException {
    messages.send(
        command(
            CommandType.START, validationKey, parameters(fileFormat, checklist), user, null));
  }

  /** Asks the engine to end an orchestrated validation's workflow. */
  public void cancel(UUID validationKey, String user) throws IOException {
    messages.send(command(CommandType.CANCEL, validationKey, Map.of(), user, "Cancelled by user"));
  }

  private WorkflowCommand command(
      CommandType type,
      UUID validationKey,
      Map<String, String> parameters,
      String user,
      String reason) {
    return new WorkflowCommand(
        UUID.randomUUID(),
        type,
        Domain.VALIDATION,
        validationKey,
        1,
        null,
        parameters,
        user,
        reason,
        clock.instant());
  }
}
