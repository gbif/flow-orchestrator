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
package org.gbif.orchestrator.ws.clb;

import java.util.Optional;

import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * The one ChecklistBank call the validation poller needs: the latest import of a dataset. The
 * import is kept as JSON, as ChecklistBank sent it, because the checklist workers read it.
 */
public class ChecklistbankClient {

  /** Import statuses (ChecklistBank's, compared case-insensitively). */
  public static final String FINISHED = "finished";

  public static final String CANCELED = "canceled";

  private final RestClient rest;

  public ChecklistbankClient(RestClient rest) {
    this.rest = rest;
  }

  public static ChecklistbankClient create(String url, String user, String password) {
    return new ChecklistbankClient(
        RestClient.builder()
            .baseUrl(url)
            .defaultHeaders(headers -> headers.setBasicAuth(user, password))
            .build());
  }

  /** {@code GET dataset/{key}/import}: the newest import, if any. */
  public Optional<JsonNode> latestImport(int datasetKey) {
    JsonNode imports =
        rest.get()
            .uri("dataset/{key}/import", datasetKey)
            .accept(MediaType.APPLICATION_JSON)
            .retrieve()
            .body(JsonNode.class);
    return imports == null || !imports.isArray() || imports.isEmpty()
        ? Optional.empty()
        : Optional.of(imports.get(0));
  }

  /** The import's status, or null. */
  public static String status(JsonNode datasetImport) {
    JsonNode status = datasetImport == null ? null : datasetImport.get("status");
    return status == null || status.isNull() ? null : status.asText();
  }
}
