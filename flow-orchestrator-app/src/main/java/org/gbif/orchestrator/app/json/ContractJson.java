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
package org.gbif.orchestrator.app.json;

import org.gbif.orchestrator.api.json.OrchestratorJson;

import java.util.Map;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * JSON for message payloads and JSON-as-text columns, using the contract settings from {@link
 * OrchestratorJson}. Deliberately not an {@link ObjectMapper} bean, so Spring Boot's own mapper
 * stays in place for HTTP and Actuator.
 */
@Component
public class ContractJson {

  private static final TypeReference<Map<String, String>> STRING_MAP = new TypeReference<>() {};

  private final ObjectMapper mapper = OrchestratorJson.mapper();

  public String write(Object value) {
    try {
      return mapper.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Cannot serialize " + value.getClass().getName(), e);
    }
  }

  public <T> T read(String json, Class<T> type) {
    try {
      return mapper.readValue(json, type);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Cannot deserialize " + type.getName(), e);
    }
  }

  /** Reads a JSON object of strings; null or empty text gives an empty map. */
  public Map<String, String> readStringMap(String json) {
    if (json == null || json.isBlank()) {
      return Map.of();
    }
    try {
      return Map.copyOf(mapper.readValue(json, STRING_MAP));
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Cannot deserialize string map", e);
    }
  }

  public ObjectMapper mapper() {
    return mapper;
  }
}
