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
import org.gbif.orchestrator.ws.service.ValidationServiceImpl;

/** Hands the engine's VALIDATOR_CHECKLISTBANK requests to the validation service. */
public class ChecklistbankRequestListener implements MessageCallback<ChecklistbankRequest> {

  private final ValidationServiceImpl validations;

  public ChecklistbankRequestListener(ValidationServiceImpl validations) {
    this.validations = validations;
  }

  @Override
  public void handleMessage(ChecklistbankRequest request) {
    validations.awaitChecklistbank(request);
  }

  @Override
  public Class<ChecklistbankRequest> getMessageClass() {
    return ChecklistbankRequest.class;
  }
}
