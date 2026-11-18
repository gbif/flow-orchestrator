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

import org.gbif.orchestrator.app.persistence.OutboxMessage;

/** Sends one outbox row to the broker and returns only once the broker has accepted it. */
public interface OutboxPublisher {

  /**
   * @throws Exception if the message was not accepted (nack, no confirm in time, unroutable, or a
   *     connection problem); the relay then backs off and tries again later
   */
  void publish(OutboxMessage message) throws Exception;
}
