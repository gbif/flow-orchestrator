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
import org.gbif.orchestrator.engine.WorkflowState.Status;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import static org.gbif.orchestrator.app.persistence.Timestamps.fromDb;
import static org.gbif.orchestrator.app.persistence.Timestamps.toDb;

/** {@code step_execution}: one row per step, updated in place. */
@Repository
public class StepExecutionRepository {

  private final JdbcClient jdbc;

  public StepExecutionRepository(JdbcClient jdbc) {
    this.jdbc = jdbc;
  }

  public void insert(StepExecution step) {
    jdbc.sql(
            """
            INSERT INTO step_execution (workflow_execution_id, step_type, step_order, status,
              current_attempt, modified, reason)
            VALUES (:workflowExecutionId, :stepType, :stepOrder, :status, :currentAttempt, :modified,
              :reason)
            """)
        .param("workflowExecutionId", step.workflowExecutionId())
        .param("stepType", step.stepType().name())
        .param("stepOrder", step.stepOrder())
        .param("status", step.status().name())
        .param("currentAttempt", step.currentAttempt())
        .param("modified", toDb(step.modified()))
        .param("reason", step.reason())
        .update();
  }

  public List<StepExecution> findByExecution(UUID workflowExecutionId) {
    return jdbc.sql(
            """
            SELECT * FROM step_execution
            WHERE workflow_execution_id = :id
            ORDER BY step_order, step_type
            """)
        .param("id", workflowExecutionId)
        .query(StepExecutionRepository::map)
        .list();
  }

  public void update(
      UUID workflowExecutionId,
      StepType stepType,
      Status status,
      int currentAttempt,
      Instant modified,
      String reason) {
    int updated =
        jdbc.sql(
                """
                UPDATE step_execution
                SET status = :status, current_attempt = :currentAttempt, modified = :modified,
                  reason = :reason
                WHERE workflow_execution_id = :id AND step_type = :stepType
                """)
            .param("status", status.name())
            .param("currentAttempt", currentAttempt)
            .param("modified", toDb(modified))
            .param("reason", reason)
            .param("id", workflowExecutionId)
            .param("stepType", stepType.name())
            .update();
    if (updated != 1) {
      throw new IllegalStateException(
          "No step " + stepType + " in workflow execution " + workflowExecutionId);
    }
  }

  private static StepExecution map(ResultSet rs, int row) throws SQLException {
    return new StepExecution(
        rs.getObject("workflow_execution_id", UUID.class),
        StepType.valueOf(rs.getString("step_type")),
        rs.getInt("step_order"),
        Status.valueOf(rs.getString("status")),
        rs.getInt("current_attempt"),
        fromDb(rs, "modified"),
        rs.getString("reason"));
  }
}
