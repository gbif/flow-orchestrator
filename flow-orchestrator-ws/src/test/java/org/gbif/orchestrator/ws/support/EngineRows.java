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
package org.gbif.orchestrator.ws.support;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;

/** Writes workflow rows as the engine (flow-orchestrator-app) would, and clears all tables. */
public final class EngineRows {

  private EngineRows() {}

  public static void clear(JdbcClient jdbc) {
    for (String table :
        new String[] {
          "validation",
          "workflow_report",
          "outbox_message",
          "workflow_command",
          "step_attempt",
          "step_execution",
          "workflow_execution"
          // not shedlock: ShedLock remembers its lock rows exist and only updates them after
        }) {
      jdbc.sql("DELETE FROM " + table).update();
    }
  }

  public static UUID execution(
      JdbcClient jdbc, UUID validationKey, String status, boolean rejected, String reason) {
    UUID id = UUID.randomUUID();
    Timestamp now = Timestamp.from(Instant.now());
    jdbc.sql(
            """
            INSERT INTO workflow_execution (id, domain, dataset_key, attempt, run, workflow_name,
              definition_hash, parameters, status, version, rejected, reason, created, modified)
            VALUES (:id, 'VALIDATION', :key, 1, 1, 'validation-dwcdp', 'h', '{}', :status, 1,
              :rejected, :reason, :now, :now)
            """)
        .param("id", id)
        .param("key", validationKey)
        .param("status", status)
        .param("rejected", rejected)
        .param("reason", reason)
        .param("now", now)
        .update();
    return id;
  }

  public static void step(
      JdbcClient jdbc, UUID executionId, String stepType, int order, String status, String reason) {
    jdbc.sql(
            """
            INSERT INTO step_execution (workflow_execution_id, step_type, step_order, status,
              current_attempt, reason, modified)
            VALUES (:id, :type, :order, :status, 1, :reason, :now)
            """)
        .param("id", executionId)
        .param("type", stepType)
        .param("order", order)
        .param("status", status)
        .param("reason", reason)
        .param("now", Timestamp.from(Instant.now()))
        .update();
  }

  /** A report as the engine stores it (jsonb; H2 parses the text with FORMAT JSON). */
  public static void report(JdbcClient jdbc, UUID validationKey, String type, String json) {
    jdbc.sql(
            """
            INSERT INTO workflow_report (dataset_key, attempt, report_type, report, created, modified)
            VALUES (:key, 1, :type, :report FORMAT JSON, :now, :now)
            """)
        .param("key", validationKey)
        .param("type", type)
        .param("report", json)
        .param("now", Timestamp.from(Instant.now()))
        .update();
  }
}
