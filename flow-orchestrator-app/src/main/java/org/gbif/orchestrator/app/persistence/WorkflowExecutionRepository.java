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

import org.gbif.orchestrator.api.command.Domain;
import org.gbif.orchestrator.app.json.ContractJson;
import org.gbif.orchestrator.engine.WorkflowState.Status;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import static org.gbif.orchestrator.app.persistence.Timestamps.fromDb;
import static org.gbif.orchestrator.app.persistence.Timestamps.toDb;

/** {@code workflow_execution}: the aggregate root. All state changes end with a version check. */
@Repository
public class WorkflowExecutionRepository {

  private final JdbcClient jdbc;
  private final ContractJson json;

  public WorkflowExecutionRepository(JdbcClient jdbc, ContractJson json) {
    this.jdbc = jdbc;
    this.json = json;
  }

  public void insert(WorkflowExecution execution) {
    jdbc.sql(
            """
            INSERT INTO workflow_execution (id, domain, dataset_key, attempt, run, workflow_name,
              definition_hash, parameters, status, version, created, modified, finished, rejected, reason)
            VALUES (:id, :domain, :datasetKey, :attempt, :run, :workflowName,
              :definitionHash, :parameters, :status, :version, :created, :modified, :finished,
              :rejected, :reason)
            """)
        .param("id", execution.id())
        .param("domain", execution.domain().name())
        .param("datasetKey", execution.datasetKey())
        .param("attempt", execution.attempt())
        .param("run", execution.run())
        .param("workflowName", execution.workflowName())
        .param("definitionHash", execution.definitionHash())
        .param("parameters", json.write(execution.parameters()))
        .param("status", execution.status().name())
        .param("version", execution.version())
        .param("created", toDb(execution.created()))
        .param("modified", toDb(execution.modified()))
        .param("finished", toDb(execution.finished()))
        .param("rejected", execution.rejected())
        .param("reason", execution.reason())
        .update();
  }

  public Optional<WorkflowExecution> find(UUID id) {
    return jdbc.sql("SELECT * FROM workflow_execution WHERE id = :id")
        .param("id", id)
        .query(this::map)
        .optional();
  }

  /** Whether a given run of a subject already exists, e.g. run 1 created by START. */
  public boolean exists(Domain domain, UUID datasetKey, int attempt, int run) {
    return jdbc.sql(
            """
            SELECT count(*) FROM workflow_execution
            WHERE domain = :domain AND dataset_key = :datasetKey AND attempt = :attempt AND run = :run
            """)
        .param("domain", domain.name())
        .param("datasetKey", datasetKey)
        .param("attempt", attempt)
        .param("run", run)
        .query(Long.class)
        .single()
        > 0;
  }

  /**
   * The latest run of a subject. Used to correlate legacy completions, which carry only the
   * dataset key and attempt.
   */
  public Optional<WorkflowExecution> findLatestRun(Domain domain, UUID datasetKey, int attempt) {
    return jdbc.sql(
            """
            SELECT * FROM workflow_execution
            WHERE domain = :domain AND dataset_key = :datasetKey AND attempt = :attempt
            ORDER BY run DESC
            LIMIT 1
            """)
        .param("domain", domain.name())
        .param("datasetKey", datasetKey)
        .param("attempt", attempt)
        .query(this::map)
        .optional();
  }

  /**
   * Writes the new status and verdict and increments the version, but only if the row still has
   * {@code expectedVersion}. Returns false when another transaction changed it in the meantime.
   */
  public boolean updateIfVersion(
      UUID id,
      int expectedVersion,
      Status status,
      Instant modified,
      Instant finished,
      boolean rejected,
      String reason) {
    return jdbc.sql(
                """
                UPDATE workflow_execution
                SET status = :status, modified = :modified, finished = :finished,
                  rejected = :rejected, reason = :reason, version = version + 1
                WHERE id = :id AND version = :version
                """)
            .param("status", status.name())
            .param("modified", toDb(modified))
            .param("finished", toDb(finished))
            .param("rejected", rejected)
            .param("reason", reason)
            .param("id", id)
            .param("version", expectedVersion)
            .update()
        == 1;
  }

  private WorkflowExecution map(ResultSet rs, int row) throws SQLException {
    return new WorkflowExecution(
        rs.getObject("id", UUID.class),
        Domain.valueOf(rs.getString("domain")),
        rs.getObject("dataset_key", UUID.class),
        rs.getInt("attempt"),
        rs.getInt("run"),
        rs.getString("workflow_name"),
        rs.getString("definition_hash"),
        json.readStringMap(rs.getString("parameters")),
        Status.valueOf(rs.getString("status")),
        rs.getInt("version"),
        fromDb(rs, "created"),
        fromDb(rs, "modified"),
        fromDb(rs, "finished"),
        rs.getBoolean("rejected"),
        rs.getString("reason"));
  }
}
