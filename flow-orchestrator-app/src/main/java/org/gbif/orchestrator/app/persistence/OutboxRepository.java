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
import org.gbif.orchestrator.api.command.Domain;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import static org.gbif.orchestrator.app.persistence.Timestamps.fromDb;
import static org.gbif.orchestrator.app.persistence.Timestamps.toDb;

/**
 * {@code outbox_message}: messages decided in a workflow transaction, published afterwards by the
 * relay. Rows belong to a domain through their workflow execution.
 */
@Repository
public class OutboxRepository {

  private final JdbcClient jdbc;

  public OutboxRepository(JdbcClient jdbc) {
    this.jdbc = jdbc;
  }

  public void insert(OutboxMessage message) {
    jdbc.sql(
            """
            INSERT INTO outbox_message (message_id, workflow_execution_id, step_type, step_attempt,
              exchange, routing_key, payload, created, published, publish_attempts, last_error,
              next_attempt)
            VALUES (:messageId, :workflowExecutionId, :stepType, :stepAttempt,
              :exchange, :routingKey, :payload, :created, :published, :publishAttempts, :lastError,
              :nextAttempt)
            """)
        .param("messageId", message.messageId())
        .param("workflowExecutionId", message.workflowExecutionId())
        .param("stepType", message.stepType() == null ? null : message.stepType().name())
        .param("stepAttempt", message.stepAttempt())
        .param("exchange", message.exchange())
        .param("routingKey", message.routingKey())
        .param("payload", message.payload())
        .param("created", toDb(message.created()))
        .param("published", toDb(message.published()))
        .param("publishAttempts", message.publishAttempts())
        .param("lastError", message.lastError())
        .param("nextAttempt", toDb(message.nextAttempt()))
        .update();
  }

  public List<OutboxMessage> findByExecution(UUID workflowExecutionId) {
    return jdbc.sql("SELECT * FROM outbox_message WHERE workflow_execution_id = :id ORDER BY id")
        .param("id", workflowExecutionId)
        .query(OutboxRepository::map)
        .list();
  }

  /**
   * Unpublished rows of the domain that are due at {@code now} (never tried, or their backoff has
   * passed), in id order.
   */
  public List<OutboxMessage> findDue(Domain domain, Instant now, int limit) {
    return jdbc.sql(
            """
            SELECT o.* FROM outbox_message o
            JOIN workflow_execution w ON w.id = o.workflow_execution_id
            WHERE o.published IS NULL AND w.domain = :domain
              AND (o.next_attempt IS NULL OR o.next_attempt <= :now)
            ORDER BY o.id
            LIMIT :limit
            """)
        .param("domain", domain.name())
        .param("now", toDb(now))
        .param("limit", limit)
        .query(OutboxRepository::map)
        .list();
  }

  public void markPublished(long id, Instant published) {
    jdbc.sql("UPDATE outbox_message SET published = :published WHERE id = :id")
        .param("published", toDb(published))
        .param("id", id)
        .update();
  }

  /** Records a failed publish and when to try again. */
  public void markFailed(long id, int publishAttempts, String lastError, Instant nextAttempt) {
    jdbc.sql(
            """
            UPDATE outbox_message
            SET publish_attempts = :publishAttempts, last_error = :lastError,
              next_attempt = :nextAttempt
            WHERE id = :id
            """)
        .param("publishAttempts", publishAttempts)
        .param("lastError", lastError)
        .param("nextAttempt", toDb(nextAttempt))
        .param("id", id)
        .update();
  }

  /** Number of unpublished rows of the domain and the creation time of the oldest (or null). */
  public Backlog backlog(Domain domain) {
    return jdbc.sql(
            """
            SELECT count(*) AS unpublished, min(o.created) AS oldest FROM outbox_message o
            JOIN workflow_execution w ON w.id = o.workflow_execution_id
            WHERE o.published IS NULL AND w.domain = :domain
            """)
        .param("domain", domain.name())
        .query((rs, row) -> new Backlog(rs.getLong("unpublished"), fromDb(rs, "oldest")))
        .single();
  }

  /** The relay's backlog: unpublished rows and the oldest one's creation time. */
  public record Backlog(long unpublished, Instant oldest) {}

  private static OutboxMessage map(ResultSet rs, int row) throws SQLException {
    String step = rs.getString("step_type");
    int stepAttempt = rs.getInt("step_attempt");
    Integer attempt = rs.wasNull() ? null : stepAttempt;
    return new OutboxMessage(
        rs.getLong("id"),
        rs.getObject("message_id", UUID.class),
        rs.getObject("workflow_execution_id", UUID.class),
        step == null ? null : StepType.valueOf(step),
        attempt,
        rs.getString("exchange"),
        rs.getString("routing_key"),
        rs.getString("payload"),
        fromDb(rs, "created"),
        fromDb(rs, "published"),
        rs.getInt("publish_attempts"),
        rs.getString("last_error"),
        fromDb(rs, "next_attempt"));
  }
}
