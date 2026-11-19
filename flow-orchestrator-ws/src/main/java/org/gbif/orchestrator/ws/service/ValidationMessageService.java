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

import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.orchestrator.api.command.CommandType;
import org.gbif.orchestrator.api.command.Domain;
import org.gbif.orchestrator.api.command.WorkflowCommand;
import org.gbif.orchestrator.ws.api.FileFormat;
import org.gbif.orchestrator.ws.messaging.Messages;

import java.io.IOException;
import java.time.Clock;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

/**
 * Starts the work of a validation. Formats the engine runs ({@link #isOrchestrated}) get a {@code
 * WorkflowCommand}; the others keep today's legacy pipelines start message until Round B.
 */
@Service
public class ValidationMessageService {

  /** The engine's workflows are matched on this parameter (see the engine's workflow config). */
  public static final String FILE_FORMAT_PARAMETER = "fileFormat";

  private final Messages messages;
  private final Clock clock;

  public ValidationMessageService(Messages messages, Clock clock) {
    this.messages = messages;
    this.clock = clock;
  }

  /** True for formats whose validation runs as an engine workflow. */
  public static boolean isOrchestrated(FileFormat fileFormat) {
    return fileFormat == FileFormat.DWCDP;
  }

  public void start(UUID validationKey, FileFormat fileFormat, Set<StepType> steps, String user)
      throws IOException {
    if (isOrchestrated(fileFormat)) {
      messages.send(
          command(
              CommandType.START,
              validationKey,
              Map.of(FILE_FORMAT_PARAMETER, fileFormat.name()),
              user,
              null));
    } else {
      PipelinesArchiveValidatorMessage message = new PipelinesArchiveValidatorMessage();
      message.setDatasetUuid(validationKey);
      message.setAttempt(1);
      message.setExecutionId(1L);
      message.setPipelineSteps(steps.stream().map(Enum::name).collect(Collectors.toSet()));
      message.setFileFormat(fileFormat.name());
      messages.send(message);
    }
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
