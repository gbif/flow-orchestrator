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

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The tables live in the orchestrator schema, and connections use it as their current schema, so
 * the repositories' unqualified SQL resolves there and nothing lands in the default schema.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class SchemaTest {

  @Autowired JdbcClient jdbc;

  @Test
  void connectionsWorkInTheOrchestratorSchema() {
    assertEquals(
        "orchestrator", jdbc.sql("SELECT current_schema()").query(String.class).single());
  }

  @Test
  void everyTableIsInTheOrchestratorSchema() {
    List<String> tables =
        jdbc.sql(
                "SELECT lower(table_name) FROM information_schema.tables"
                    + " WHERE lower(table_schema) = 'orchestrator' ORDER BY 1")
            .query(String.class)
            .list();

    assertEquals(
        List.of(
            "outbox_message",
            "step_attempt",
            "step_execution",
            "workflow_command",
            "workflow_execution",
            "workflow_report"),
        tables);
  }
}
