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

import org.gbif.orchestrator.app.json.ContractJson;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.databind.JsonNode;

import static org.gbif.orchestrator.app.persistence.Timestamps.fromDb;
import static org.gbif.orchestrator.app.persistence.Timestamps.toDb;

/**
 * {@code workflow_report}: reports keyed by subject, attempt and type. A newer report of the same
 * type replaces the older one, so repeated processing is safe.
 */
@Repository
public class WorkflowReportRepository {

  private final JdbcClient jdbc;
  private final ContractJson json;
  private final JsonSql jsonSql;

  public WorkflowReportRepository(JdbcClient jdbc, ContractJson json, JsonSql jsonSql) {
    this.jdbc = jdbc;
    this.json = json;
    this.jsonSql = jsonSql;
  }

  /**
   * Inserts the report, or replaces the content and comment of the existing one (keeping its
   * {@code created}). Written as update-then-insert to stay portable; a concurrent insert of the
   * same key fails with a duplicate key, which the callers retry.
   */
  public void upsert(WorkflowReport report) {
    String content = json.write(report.report());
    int updated =
        jdbc.sql(
                """
                UPDATE workflow_report
                SET report = %s, comment = :comment, modified = :modified
                WHERE dataset_key = :datasetKey AND attempt = :attempt
                  AND report_type = :reportType
                """
                    .formatted(jsonSql.parameter("report")))
            .param("report", content)
            .param("comment", report.comment())
            .param("modified", toDb(report.modified()))
            .param("datasetKey", report.datasetKey())
            .param("attempt", report.attempt())
            .param("reportType", report.reportType())
            .update();
    if (updated == 0) {
      jdbc.sql(
              """
              INSERT INTO workflow_report (dataset_key, attempt, report_type, report, comment,
                created, modified)
              VALUES (:datasetKey, :attempt, :reportType, %s, :comment, :created, :modified)
              """
                  .formatted(jsonSql.parameter("report")))
          .param("datasetKey", report.datasetKey())
          .param("attempt", report.attempt())
          .param("reportType", report.reportType())
          .param("report", content)
          .param("comment", report.comment())
          .param("created", toDb(report.created()))
          .param("modified", toDb(report.modified()))
          .update();
    }
  }

  /** All reports of a subject attempt, ordered by type. */
  public List<WorkflowReport> findBySubject(UUID datasetKey, int attempt) {
    return jdbc.sql(
            """
            SELECT dataset_key, attempt, report_type, CAST(report AS text) AS report, comment,
              created, modified
            FROM workflow_report
            WHERE dataset_key = :datasetKey AND attempt = :attempt
            ORDER BY report_type
            """)
        .param("datasetKey", datasetKey)
        .param("attempt", attempt)
        .query(this::map)
        .list();
  }

  /** The report types stored for a subject attempt, ordered by type. */
  public List<String> findTypes(UUID datasetKey, int attempt) {
    return jdbc.sql(
            """
            SELECT report_type FROM workflow_report
            WHERE dataset_key = :datasetKey AND attempt = :attempt
            ORDER BY report_type
            """)
        .param("datasetKey", datasetKey)
        .param("attempt", attempt)
        .query(String.class)
        .list();
  }

  private WorkflowReport map(ResultSet rs, int row) throws SQLException {
    return new WorkflowReport(
        rs.getObject("dataset_key", UUID.class),
        rs.getInt("attempt"),
        rs.getString("report_type"),
        json.read(rs.getString("report"), JsonNode.class),
        rs.getString("comment"),
        fromDb(rs, "created"),
        fromDb(rs, "modified"));
  }
}
