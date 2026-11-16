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
package org.gbif.orchestrator.app;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Starts the production application and checks that Actuator is reachable without credentials. */
// Metrics exporters (Prometheus) are disabled in tests unless explicitly enabled.
@AutoConfigureObservability
@SpringBootTest(classes = Main.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ActuatorIntegrationTest {

  @LocalServerPort private int port;
  @Autowired private ObjectMapper mapper;

  private final HttpClient http =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();

  @Test
  void healthProbesAreUp() throws Exception {
    assertEquals("UP", get("/actuator/health/liveness", 200).path("status").asText());
    assertEquals("UP", get("/actuator/health/readiness", 200).path("status").asText());
  }

  @Test
  void prometheusEndpointIsExposed() throws Exception {
    send("/actuator/prometheus", 200);
  }

  @Test
  void nonActuatorRequestsAreDenied() throws Exception {
    send("/anything", 403);
  }

  private JsonNode get(String path, int expectedStatus) throws Exception {
    return mapper.readTree(send(path, expectedStatus));
  }

  private String send(String path, int expectedStatus) throws Exception {
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:" + port + path))
            .timeout(Duration.ofSeconds(5))
            .GET()
            .build();
    HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
    assertEquals(expectedStatus, response.statusCode(), path + " returned " + response.body());
    return response.body();
  }
}
