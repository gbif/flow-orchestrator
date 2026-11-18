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
import org.gbif.orchestrator.app.persistence.OutboxWritten;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.transaction.event.TransactionalEventListener;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Publishes committed outbox rows of this instance's domain, in id order.
 *
 * <ul>
 *   <li>Runs on one thread: every poll interval, and right after a transaction that wrote outbox
 *       rows commits ({@link OutboxWritten}).
 *   <li>A row is marked published only after the broker confirmed it. A failed row gets an
 *       exponential backoff (1s, 2s, 4s… up to {@code max-backoff}); the rows after it still go
 *       out.
 *   <li>A crash between the confirm and marking the row gives a duplicate send, never a lost one.
 *       So do several instances of the same domain; receivers deduplicate.
 * </ul>
 */
public class OutboxRelay implements SmartLifecycle {

  private static final Logger LOG = LoggerFactory.getLogger(OutboxRelay.class);
  private static final int MAX_ERROR_LENGTH = 2000;

  private final OutboxRepository outbox;
  private final OutboxPublisher publisher;
  private final Domain domain;
  private final OrchestratorProperties.Relay settings;
  private final Clock clock;
  private final Counter failures;

  private ScheduledExecutorService executor;

  public OutboxRelay(
      OutboxRepository outbox,
      OutboxPublisher publisher,
      OrchestratorProperties properties,
      Clock clock,
      MeterRegistry meters) {
    this.outbox = outbox;
    this.publisher = publisher;
    this.domain = properties.getDomain();
    this.settings = properties.getRelay();
    this.clock = clock;
    this.failures =
        Counter.builder("orchestrator.outbox.publish.failures")
            .description("Outbox messages the broker did not accept")
            .register(meters);
    Gauge.builder("orchestrator.outbox.unpublished", () -> outbox.backlog(domain).unpublished())
        .description("Outbox messages not yet published")
        .register(meters);
    Gauge.builder("orchestrator.outbox.oldest.unpublished.age.seconds", this::oldestAgeSeconds)
        .description("Age of the oldest unpublished outbox message, 0 when there is none")
        .register(meters);
  }

  /**
   * Publishes the due rows of one batch.
   *
   * @return the number of rows published
   */
  public int publishDue() {
    Instant now = clock.instant();
    List<OutboxMessage> due = outbox.findDue(domain, now, settings.getBatchSize());
    int published = 0;
    for (OutboxMessage message : due) {
      try {
        publisher.publish(message);
        outbox.markPublished(message.id(), clock.instant());
        published++;
        LOG.debug(
            "Published outbox id={} messageId={} exchange={} routingKey={}",
            message.id(),
            message.messageId(),
            message.exchange(),
            message.routingKey());
      } catch (Exception e) {
        int attempts = message.publishAttempts() + 1;
        Instant next = now.plus(backoff(attempts));
        outbox.markFailed(message.id(), attempts, error(e), next);
        failures.increment();
        LOG.warn(
            "Publishing outbox id={} messageId={} exchange={} routingKey={} failed (attempt {}),"
                + " next try at {}: {}",
            message.id(),
            message.messageId(),
            message.exchange(),
            message.routingKey(),
            attempts,
            next,
            e.toString());
      }
    }
    return published;
  }

  /** Delay before the next try after {@code attempts} failures: 1s, 2s, 4s… capped. */
  Duration backoff(int attempts) {
    Duration max = settings.getMaxBackoff();
    if (attempts >= 31) {
      return max;
    }
    Duration delay = Duration.ofSeconds(1L << Math.max(0, attempts - 1));
    return delay.compareTo(max) > 0 ? max : delay;
  }

  /** Runs a publish pass soon, after a transaction with new outbox rows committed. */
  @TransactionalEventListener
  public void onOutboxWritten(OutboxWritten event) {
    ScheduledExecutorService current = executor;
    if (current != null) {
      current.execute(this::drain);
    }
  }

  private void drain() {
    try {
      // a full batch may mean more rows are waiting
      while (publishDue() == settings.getBatchSize()) {
        LOG.debug("Full outbox batch published, continuing");
      }
    } catch (RuntimeException e) {
      LOG.warn("Outbox relay pass failed: {}", e.toString());
    }
  }

  private double oldestAgeSeconds() {
    Instant oldest = outbox.backlog(domain).oldest();
    return oldest == null ? 0 : Duration.between(oldest, clock.instant()).toMillis() / 1000.0;
  }

  private static String error(Exception e) {
    String text = e.toString();
    return text.length() > MAX_ERROR_LENGTH ? text.substring(0, MAX_ERROR_LENGTH) : text;
  }

  @Override
  public synchronized void start() {
    if (executor == null) {
      executor =
          Executors.newSingleThreadScheduledExecutor(
              r -> {
                Thread thread = new Thread(r, "outbox-relay");
                thread.setDaemon(true);
                return thread;
              });
      long interval = settings.getPollInterval().toMillis();
      executor.scheduleWithFixedDelay(this::drain, 0, interval, TimeUnit.MILLISECONDS);
      LOG.info(
          "Outbox relay started domain={} pollInterval={} batchSize={}",
          domain,
          settings.getPollInterval(),
          settings.getBatchSize());
    }
  }

  @Override
  public synchronized void stop() {
    if (executor != null) {
      executor.shutdown();
      try {
        if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
          executor.shutdownNow();
        }
      } catch (InterruptedException e) {
        executor.shutdownNow();
        Thread.currentThread().interrupt();
      }
      executor = null;
      LOG.info("Outbox relay stopped");
    }
  }

  @Override
  public synchronized boolean isRunning() {
    return executor != null;
  }
}
