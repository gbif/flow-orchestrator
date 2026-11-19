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
package org.gbif.orchestrator.ws.support;

import org.gbif.orchestrator.ws.clb.ChecklistbankClient;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/** Handwritten fakes for messaging, mail and ChecklistBank. */
@TestConfiguration
public class TestBeans {

  @Bean
  public FakeMessages messages() {
    return new FakeMessages();
  }

  @Bean
  @Primary
  public RecordingEmails recordingEmails() {
    return new RecordingEmails();
  }

  @Bean
  @Primary
  public FakeChecklistbank fakeChecklistbank() {
    return new FakeChecklistbank();
  }

  /** Answers imports from a map; an unknown dataset key throws, like an unreachable API. */
  public static class FakeChecklistbank extends ChecklistbankClient {
    public final java.util.Map<Integer, com.fasterxml.jackson.databind.JsonNode> imports =
        new java.util.concurrent.ConcurrentHashMap<>();

    public FakeChecklistbank() {
      super(null);
    }

    @Override
    public java.util.Optional<com.fasterxml.jackson.databind.JsonNode> latestImport(
        int datasetKey) {
      com.fasterxml.jackson.databind.JsonNode found = imports.get(datasetKey);
      if (found == null) {
        throw new IllegalStateException("ChecklistBank unreachable");
      }
      return java.util.Optional.of(found);
    }
  }
}
