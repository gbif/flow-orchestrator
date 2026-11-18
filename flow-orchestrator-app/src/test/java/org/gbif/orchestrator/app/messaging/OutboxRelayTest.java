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

import org.gbif.orchestrator.api.command.Domain;
import org.gbif.orchestrator.app.config.OrchestratorProperties;
import org.gbif.orchestrator.app.persistence.OutboxMessage;
import org.gbif.orchestrator.app.persistence.OutboxRepository;
import org.gbif.orchestrator.app.persistence.WorkflowExecution;
import org.gbif.orchestrator.app.persistence.WorkflowExecutionRepository;
import org.gbif.orchestrator.app.support.Database;
import org.gbif.orchestrator.app.support.MutableClock;
import org.gbif.orchestrator.engine.WorkflowState.Status;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/** The relay against the H2 outbox, with a fake broker. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class OutboxRelayTest {

  private static final Instant T0 = Instant.parse("2026-11-16T12:00:00Z");

  @Autowired JdbcClient jdbc;
  @Autowired OutboxRepository outbox;
  @Autowired WorkflowExecutionRepository executions;

  private final FakePublisher publisher = new FakePublisher();
  private final MutableClock clock = new MutableClock(T0);
  private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
  private OutboxRelay relay;
  private UUID validation;
  private UUID ingestion;

  @BeforeEach
  void setup() {
    Database.clear(jdbc);
    OrchestratorProperties properties = new OrchestratorProperties();
    properties.setDomain(Domain.VALIDATION);
    properties.getRelay().setBatchSize(10);
    properties.getRelay().setMaxBackoff(Duration.ofSeconds(30));
    relay = new OutboxRelay(outbox, publisher, properties, clock, meters);
    validation = execution(Domain.VALIDATION);
    ingestion = execution(Domain.INGESTION);
  }

  @Test
  void publishesDueRowsOfItsDomainInOrder() {
    OutboxMessage first = insert(validation, "a");
    insert(ingestion, "other domain");
    OutboxMessage second = insert(validation, "b");

    assertEquals(2, relay.publishDue());

    assertEquals(List.of(first.messageId(), second.messageId()), publisher.sentIds());
    assertEquals("{\"value\":\"a\"}", publisher.sent.get(0).payload());
    for (OutboxMessage stored : outbox.findByExecution(validation)) {
      assertEquals(T0, stored.published());
    }
    assertNull(outbox.findByExecution(ingestion).get(0).published());
    assertEquals(0, relay.publishDue(), "nothing left");
    assertEquals(0.0, meters.get("orchestrator.outbox.unpublished").gauge().value());
  }

  @Test
  void failureBacksOffWithoutBlockingOtherRows() {
    OutboxMessage failing = insert(validation, "a");
    OutboxMessage next = insert(validation, "b");
    publisher.failing.add(failing.messageId());

    assertEquals(1, relay.publishDue());

    OutboxMessage stored = find(failing);
    assertNull(stored.published());
    assertEquals(1, stored.publishAttempts());
    assertEquals(T0.plusSeconds(1), stored.nextAttempt());
    assertNotNull(stored.lastError());
    assertNotNull(find(next).published());
    assertEquals(1.0, meters.get("orchestrator.outbox.publish.failures").counter().count());
    assertEquals(1.0, meters.get("orchestrator.outbox.unpublished").gauge().value());

    assertEquals(0, relay.publishDue(), "still backing off");
    clock.advance(Duration.ofSeconds(1));
    assertEquals(0, relay.publishDue(), "fails again");
    assertEquals(2, find(failing).publishAttempts());
    assertEquals(T0.plusSeconds(3), find(failing).nextAttempt(), "1s, then 2s");
    assertEquals(
        1.0, meters.get("orchestrator.outbox.oldest.unpublished.age.seconds").gauge().value());

    publisher.failing.clear();
    clock.advance(Duration.ofSeconds(2));
    assertEquals(1, relay.publishDue());
    assertNotNull(find(failing).published());
  }

  @Test
  void backoffDoublesUpToTheMaximum() {
    assertEquals(Duration.ofSeconds(1), relay.backoff(1));
    assertEquals(Duration.ofSeconds(2), relay.backoff(2));
    assertEquals(Duration.ofSeconds(16), relay.backoff(5));
    assertEquals(Duration.ofSeconds(30), relay.backoff(6));
    assertEquals(Duration.ofSeconds(30), relay.backoff(100));
  }

  private OutboxMessage find(OutboxMessage message) {
    return outbox.findByExecution(message.workflowExecutionId()).stream()
        .filter(m -> m.messageId().equals(message.messageId()))
        .findFirst()
        .orElseThrow();
  }

  private OutboxMessage insert(UUID execution, String value) {
    OutboxMessage message =
        new OutboxMessage(
            null,
            UUID.randomUUID(),
            execution,
            null,
            null,
            "orchestrator",
            "rk",
            "{\"value\":\"" + value + "\"}",
            T0,
            null,
            0,
            null,
            null);
    outbox.insert(message);
    return message;
  }

  private UUID execution(Domain domain) {
    UUID id = UUID.randomUUID();
    executions.insert(
        new WorkflowExecution(
            id,
            domain,
            UUID.randomUUID(),
            1,
            1,
            "w",
            "h",
            Map.of(),
            Status.RUNNING,
            0,
            T0,
            T0,
            null,
            false,
            null));
    return id;
  }

  /** Records what was sent; fails for chosen message ids, as a nack or unroutable would. */
  static class FakePublisher implements OutboxPublisher {
    final List<OutboxMessage> sent = new ArrayList<>();
    final Set<UUID> failing = new HashSet<>();

    @Override
    public void publish(OutboxMessage message) throws Exception {
      if (failing.contains(message.messageId())) {
        throw new java.io.IOException("returned by broker: 312 NO_ROUTE");
      }
      sent.add(message);
    }

    List<UUID> sentIds() {
      return sent.stream().map(OutboxMessage::messageId).toList();
    }
  }
}
