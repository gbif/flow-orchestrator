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
import org.gbif.orchestrator.api.step.Outcome;
import org.gbif.orchestrator.app.json.ContractJson;
import org.gbif.orchestrator.engine.WorkflowState.Status;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import static org.gbif.orchestrator.app.persistence.Timestamps.fromDb;
import static org.gbif.orchestrator.app.persistence.Timestamps.toDb;

/** {@code step_attempt}: one row per try of a step; the history of the step. */
@Repository
public class StepAttemptRepository {

  private final JdbcClient jdbc;
  private final ContractJson json;

  public StepAttemptRepository(JdbcClient jdbc, ContractJson json) {
    this.jdbc = jdbc;
    this.json = json;
  }

  public void insert(StepAttempt attempt) {
    jdbc.sql(
            """
            INSERT INTO step_attempt (workflow_execution_id, step_type, attempt_no, status, outcome,
              diagnostic, outputs, created, started, finished)
            VALUES (:workflowExecutionId, :stepType, :attemptNo, :status, :outcome,
              :diagnostic, :outputs, :created, :started, :finished)
            """)
        .param("workflowExecutionId", attempt.workflowExecutionId())
        .param("stepType", attempt.stepType().name())
        .param("attemptNo", attempt.attemptNo())
        .param("status", attempt.status().name())
        .param("outcome", attempt.outcome() == null ? null : attempt.outcome().name())
        .param("diagnostic", attempt.diagnostic())
        .param("outputs", attempt.outputs().isEmpty() ? null : json.write(attempt.outputs()))
        .param("created", toDb(attempt.created()))
        .param("started", toDb(attempt.started()))
        .param("finished", toDb(attempt.finished()))
        .update();
  }

  /**
   * Ends a dispatched attempt that will not be completed (its step was cancelled): status {@code
   * ABORTED}, no worker outcome, the reason as diagnostic.
   */
  public void abort(
      UUID workflowExecutionId, StepType stepType, int attemptNo, String reason, Instant finished) {
    jdbc.sql(
            """
            UPDATE step_attempt
            SET status = :status, diagnostic = :diagnostic, finished = :finished
            WHERE workflow_execution_id = :id AND step_type = :stepType
              AND attempt_no = :attemptNo AND finished IS NULL
            """)
        .param("status", Status.ABORTED.name())
        .param("diagnostic", reason)
        .param("finished", toDb(finished))
        .param("id", workflowExecutionId)
        .param("stepType", stepType.name())
        .param("attemptNo", attemptNo)
        .update();
  }

  /**
   * Records the end of an attempt: its status, the worker's outcome, diagnostic, outputs and timing.
   */
  public void complete(
      UUID workflowExecutionId,
      StepType stepType,
      int attemptNo,
      Status status,
      Outcome outcome,
      String diagnostic,
      Map<String, String> outputs,
      Instant started,
      Instant finished) {
    int updated =
        jdbc.sql(
                """
                UPDATE step_attempt
                SET status = :status, outcome = :outcome, diagnostic = :diagnostic,
                  outputs = :outputs, started = :started, finished = :finished
                WHERE workflow_execution_id = :id AND step_type = :stepType
                  AND attempt_no = :attemptNo
                """)
            .param("status", status.name())
            .param("outcome", outcome == null ? null : outcome.name())
            .param("diagnostic", diagnostic)
            .param("outputs", outputs == null || outputs.isEmpty() ? null : json.write(outputs))
            .param("started", toDb(started))
            .param("finished", toDb(finished))
            .param("id", workflowExecutionId)
            .param("stepType", stepType.name())
            .param("attemptNo", attemptNo)
            .update();
    if (updated != 1) {
      throw new IllegalStateException(
          "No attempt " + attemptNo + " of " + stepType + " in workflow " + workflowExecutionId);
    }
  }

  public Optional<StepAttempt> find(UUID workflowExecutionId, StepType stepType, int attemptNo) {
    return jdbc.sql(
            """
            SELECT * FROM step_attempt
            WHERE workflow_execution_id = :id AND step_type = :stepType AND attempt_no = :attemptNo
            """)
        .param("id", workflowExecutionId)
        .param("stepType", stepType.name())
        .param("attemptNo", attemptNo)
        .query(this::map)
        .optional();
  }

  public List<StepAttempt> findByExecution(UUID workflowExecutionId) {
    return jdbc.sql(
            """
            SELECT * FROM step_attempt
            WHERE workflow_execution_id = :id
            ORDER BY step_type, attempt_no
            """)
        .param("id", workflowExecutionId)
        .query(this::map)
        .list();
  }

  private StepAttempt map(ResultSet rs, int row) throws SQLException {
    String outcome = rs.getString("outcome");
    return new StepAttempt(
        rs.getObject("workflow_execution_id", UUID.class),
        StepType.valueOf(rs.getString("step_type")),
        rs.getInt("attempt_no"),
        Status.valueOf(rs.getString("status")),
        outcome == null ? null : Outcome.valueOf(outcome),
        rs.getString("diagnostic"),
        json.readStringMap(rs.getString("outputs")),
        fromDb(rs, "created"),
        fromDb(rs, "started"),
        fromDb(rs, "finished"));
  }
}
