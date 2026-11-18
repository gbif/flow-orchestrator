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
package org.gbif.orchestrator.app.persistence;

import org.gbif.api.model.pipelines.StepType;

import java.time.Instant;
import java.util.UUID;

/**
 * A row of {@code outbox_message}: a message decided in a committed transaction and waiting to be
 * published (or already published, once {@code published} is set).
 *
 * @param id database id; null before insert. Publishing happens in id order.
 * @param messageId sent as the AMQP message id so that receivers can drop duplicates
 */
public record OutboxMessage(
    Long id,
    UUID messageId,
    UUID workflowExecutionId,
    StepType stepType,
    Integer stepAttempt,
    String exchange,
    String routingKey,
    String payload,
    Instant created,
    Instant published,
    int publishAttempts,
    String lastError,
    Instant nextAttempt) {}
