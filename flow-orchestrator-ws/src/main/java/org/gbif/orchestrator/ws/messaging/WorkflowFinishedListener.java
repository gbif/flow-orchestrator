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

import org.gbif.common.messaging.api.MessageCallback;
import org.gbif.orchestrator.api.event.WorkflowFinished;
import org.gbif.orchestrator.ws.service.ValidationServiceImpl;

/** Hands the engine's WorkflowFinished events to the validation service. */
public class WorkflowFinishedListener implements MessageCallback<WorkflowFinished> {

  private final ValidationServiceImpl validations;

  public WorkflowFinishedListener(ValidationServiceImpl validations) {
    this.validations = validations;
  }

  @Override
  public void handleMessage(WorkflowFinished event) {
    validations.workflowFinished(event);
  }

  @Override
  public Class<WorkflowFinished> getMessageClass() {
    return WorkflowFinished.class;
  }
}
