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
package org.gbif.orchestrator.app.support;

import org.springframework.jdbc.core.simple.JdbcClient;

/** Test helper: empties the orchestrator tables between tests. */
public final class Database {

  private Database() {}

  public static void clear(JdbcClient jdbc) {
    for (String table :
        new String[] {
          "workflow_report",
          "outbox_message",
          "workflow_command",
          "step_attempt",
          "step_execution",
          "workflow_execution"
        }) {
      jdbc.sql("DELETE FROM " + table).update();
    }
  }

  public static long count(JdbcClient jdbc, String table) {
    return jdbc.sql("SELECT count(*) FROM " + table).query(Long.class).single();
  }
}
