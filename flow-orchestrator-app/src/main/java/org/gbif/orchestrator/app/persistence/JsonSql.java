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

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * SQL for binding a JSON text parameter to a {@code jsonb} column.
 *
 * <p>PostgreSQL needs an explicit cast ({@code CAST(:p AS jsonb)}). H2, used in tests, accepts the
 * {@code jsonb} type but stores a cast string as a JSON <em>string</em>; it needs {@code :p FORMAT
 * JSON} instead. This is the one SQL difference between production and the tests, set with {@code
 * orchestrator.database.json-parameter} (a format with {@code %s} for the parameter name).
 */
@Component
public class JsonSql {

  private final String format;

  public JsonSql(
      @Value("${orchestrator.database.json-parameter:CAST(:%s AS jsonb)}") String format) {
    if (!format.contains("%s")) {
      throw new IllegalStateException(
          "orchestrator.database.json-parameter must contain %s: " + format);
    }
    this.format = format;
  }

  /** The SQL expression that binds the named parameter as JSON. */
  public String parameter(String name) {
    return String.format(format, name);
  }
}
