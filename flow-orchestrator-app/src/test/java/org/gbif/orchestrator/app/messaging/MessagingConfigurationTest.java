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

import org.gbif.orchestrator.app.config.OrchestratorProperties;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Messaging settings are checked before connecting, so a bad configuration stops startup. */
class MessagingConfigurationTest {

  @Test
  void completeSettingsAreAccepted() {
    assertDoesNotThrow(() -> MessagingConfiguration.validate(valid()));
  }

  @Test
  void missingDeadLetterExchangeIsRefused() {
    OrchestratorProperties.Messaging messaging = valid();
    messaging.setDeadLetterExchange(null);
    assertThrows(IllegalStateException.class, () -> MessagingConfiguration.validate(messaging));
  }

  @Test
  void emptyPrefixIsRefused() {
    // what application.yml gives without ORCHESTRATOR_MESSAGING_PREFIX
    OrchestratorProperties.Messaging messaging = valid();
    messaging.getCommands().setQueue("orchestrator..commands");
    IllegalStateException e =
        assertThrows(
            IllegalStateException.class, () -> MessagingConfiguration.validate(messaging));
    assertTrue(e.getMessage().contains("commands"), e.getMessage());

    OrchestratorProperties.Messaging other = valid();
    other.getCompletions().setRoutingKey(".step.completed");
    assertThrows(IllegalStateException.class, () -> MessagingConfiguration.validate(other));
  }

  @Test
  void threadsMustBePositive() {
    OrchestratorProperties.Messaging messaging = valid();
    messaging.getCompletions().setThreads(0);
    assertThrows(IllegalStateException.class, () -> MessagingConfiguration.validate(messaging));
  }

  @Test
  void analysisListenerIsOptionalButMustBeCompleteWhenSet() {
    OrchestratorProperties.Messaging unset = valid();
    assertDoesNotThrow(() -> MessagingConfiguration.validate(unset));

    OrchestratorProperties.Messaging partial = valid();
    partial.getDwcdpAnalysisFinished().setQueue("orchestrator.validation.dwcdp.analysis.finished");
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> MessagingConfiguration.validate(partial));
    assertTrue(e.getMessage().contains("dwcdp-analysis-finished"), e.getMessage());

    OrchestratorProperties.Messaging complete = valid();
    complete.getDwcdpAnalysisFinished().setExchange("orchestrator");
    complete.getDwcdpAnalysisFinished().setRoutingKey("validation.dwcdp.analysis.finished");
    complete.getDwcdpAnalysisFinished().setQueue("orchestrator.validation.dwcdp.analysis.finished");
    assertDoesNotThrow(() -> MessagingConfiguration.validate(complete));
  }

  private static OrchestratorProperties.Messaging valid() {
    OrchestratorProperties.Messaging messaging = new OrchestratorProperties.Messaging();
    messaging.setDeadLetterExchange("orchestrator.dlx");
    messaging.getCommands().setExchange("orchestrator");
    messaging.getCommands().setRoutingKey("validation.command");
    messaging.getCommands().setQueue("orchestrator.validation.commands");
    messaging.getCompletions().setExchange("orchestrator");
    messaging.getCompletions().setRoutingKey("validation.step.completed");
    messaging.getCompletions().setQueue("orchestrator.validation.step.completed");
    return messaging;
  }
}
