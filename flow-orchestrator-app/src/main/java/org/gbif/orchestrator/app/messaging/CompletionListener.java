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
import org.gbif.orchestrator.api.step.StepCompleted;
import org.gbif.orchestrator.app.completion.CompletionService;

/**
 * Consumes step completions for every step of this domain. Processed and stale completions are
 * acknowledged; a completion that cannot belong here, or keeps conflicting, is dead-lettered.
 */
public class CompletionListener implements MessageCallback<StepCompleted> {

  private final CompletionService completions;

  public CompletionListener(CompletionService completions) {
    this.completions = completions;
  }

  @Override
  public void handleMessage(StepCompleted completed) {
    completions.handle(completed);
  }

  @Override
  public Class<StepCompleted> getMessageClass() {
    return StepCompleted.class;
  }
}
