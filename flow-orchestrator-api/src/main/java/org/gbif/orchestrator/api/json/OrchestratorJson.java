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
package org.gbif.orchestrator.api.json;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Jackson settings for the orchestrator contracts. Consumers that use their own {@link
 * ObjectMapper} must at least register {@link JavaTimeModule}; the other settings keep the wire
 * format readable and tolerant of newer senders.
 */
public final class OrchestratorJson {

  private OrchestratorJson() {}

  /** A new mapper configured for the contracts. */
  public static ObjectMapper mapper() {
    return configure(new ObjectMapper());
  }

  /** Configures an existing mapper for the contracts and returns it. */
  public static ObjectMapper configure(ObjectMapper mapper) {
    return mapper
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .setSerializationInclusion(JsonInclude.Include.NON_NULL);
  }
}
