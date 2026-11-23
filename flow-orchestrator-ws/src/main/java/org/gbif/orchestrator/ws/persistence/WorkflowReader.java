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
package org.gbif.orchestrator.ws.persistence;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Read-only view of the engine's tables (written only by flow-orchestrator-app) for one
 * validation: its latest workflow run, the steps and the reports.
 */
@Repository
public class WorkflowReader {

  /** The engine's domain for validations; the subject is (validation key, attempt 1). */
  static final String DOMAIN = "VALIDATION";

  public static final int ATTEMPT = 1;

  public record Execution(
      UUID id, String status, boolean rejected, String reason, String workflowName) {}

  public record Step(String stepType, int stepOrder, String status, String reason) {}

  private final JdbcClient jdbc;

  public WorkflowReader(JdbcClient jdbc) {
    this.jdbc = jdbc;
  }

  /** The latest run of the validation's workflow, if it was started through the engine. */
  public Optional<Execution> latest(UUID validationKey) {
    return jdbc.sql(
            """
            SELECT id, status, rejected, reason, workflow_name FROM workflow_execution
            WHERE domain = :domain AND dataset_key = :key AND attempt = :attempt
            ORDER BY run DESC LIMIT 1
            """)
        .param("domain", DOMAIN)
        .param("key", validationKey)
        .param("attempt", ATTEMPT)
        .query(
            (rs, row) ->
                new Execution(
                    rs.getObject("id", UUID.class),
                    rs.getString("status"),
                    rs.getBoolean("rejected"),
                    rs.getString("reason"),
                    rs.getString("workflow_name")))
        .optional();
  }

  public List<Step> steps(UUID executionId) {
    return jdbc.sql(
            """
            SELECT step_type, step_order, status, reason FROM step_execution
            WHERE workflow_execution_id = :id ORDER BY step_order, step_type
            """)
        .param("id", executionId)
        .query(
            (rs, row) ->
                new Step(
                    rs.getString("step_type"),
                    rs.getInt("step_order"),
                    rs.getString("status"),
                    rs.getString("reason")))
        .list();
  }

  /** One report of the validation, as JSON text. */
  public Optional<String> report(UUID validationKey, int attempt, String reportType) {
    return jdbc.sql(
            """
            SELECT CAST(report AS varchar) FROM workflow_report
            WHERE dataset_key = :key AND attempt = :attempt AND report_type = :type
            """)
        .param("key", validationKey)
        .param("attempt", attempt)
        .param("type", reportType)
        .query(String.class)
        .optional();
  }
}
