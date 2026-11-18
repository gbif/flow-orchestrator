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
import org.gbif.orchestrator.api.command.CommandType;
import org.gbif.orchestrator.api.command.Domain;
import org.gbif.orchestrator.api.command.WorkflowCommand;
import org.gbif.orchestrator.app.json.ContractJson;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import static org.gbif.orchestrator.app.persistence.Timestamps.fromDb;
import static org.gbif.orchestrator.app.persistence.Timestamps.toDb;

/** {@code workflow_command}: every received command and its outcome. Insert-only. */
@Repository
public class CommandRepository {

  private final JdbcClient jdbc;
  private final ContractJson json;

  public CommandRepository(JdbcClient jdbc, ContractJson json) {
    this.jdbc = jdbc;
    this.json = json;
  }

  public boolean exists(UUID commandId) {
    return jdbc.sql("SELECT count(*) FROM workflow_command WHERE command_id = :id")
            .param("id", commandId)
            .query(Long.class)
            .single()
        > 0;
  }

  public void insert(CommandRecord record) {
    WorkflowCommand command = record.command();
    jdbc.sql(
            """
            INSERT INTO workflow_command (command_id, command_type, domain, dataset_key, attempt,
              step_type, parameters, requested_by, reason, requested, received,
              workflow_execution_id, outcome, outcome_reason)
            VALUES (:commandId, :commandType, :domain, :datasetKey, :attempt,
              :stepType, :parameters, :requestedBy, :reason, :requested, :received,
              :workflowExecutionId, :outcome, :outcomeReason)
            """)
        .param("commandId", command.commandId())
        .param("commandType", command.type().name())
        .param("domain", command.domain() == null ? null : command.domain().name())
        .param("datasetKey", command.datasetKey())
        .param("attempt", command.attempt())
        .param("stepType", command.step() == null ? null : command.step().name())
        .param("parameters", json.write(command.parameters()))
        .param("requestedBy", command.requestedBy())
        .param("reason", command.reason())
        .param("requested", toDb(command.requestedAt()))
        .param("received", toDb(record.received()))
        .param("workflowExecutionId", record.workflowExecutionId())
        .param("outcome", record.outcome().name())
        .param("outcomeReason", record.outcomeReason())
        .update();
  }

  public Optional<CommandRecord> find(UUID commandId) {
    return jdbc.sql("SELECT * FROM workflow_command WHERE command_id = :id")
        .param("id", commandId)
        .query(this::map)
        .optional();
  }

  private CommandRecord map(ResultSet rs, int row) throws SQLException {
    String domain = rs.getString("domain");
    String step = rs.getString("step_type");
    WorkflowCommand command =
        new WorkflowCommand(
            rs.getObject("command_id", UUID.class),
            CommandType.valueOf(rs.getString("command_type")),
            domain == null ? null : Domain.valueOf(domain),
            rs.getObject("dataset_key", UUID.class),
            rs.getInt("attempt"),
            step == null ? null : StepType.valueOf(step),
            json.readStringMap(rs.getString("parameters")),
            rs.getString("requested_by"),
            rs.getString("reason"),
            fromDb(rs, "requested"));
    return new CommandRecord(
        command,
        fromDb(rs, "received"),
        rs.getObject("workflow_execution_id", UUID.class),
        CommandOutcome.valueOf(rs.getString("outcome")),
        rs.getString("outcome_reason"));
  }
}
