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
package org.gbif.orchestrator.ws.jobs;

import org.gbif.common.messaging.api.messages.PipelinesChecklistValidatorMessage;
import org.gbif.common.messaging.api.messages.PipelinesCleanerMessage;
import org.gbif.orchestrator.ws.api.FileFormat;
import org.gbif.orchestrator.ws.api.Validation;
import org.gbif.orchestrator.ws.api.Validation.Status;
import org.gbif.orchestrator.ws.persistence.ValidationRepository;
import org.gbif.orchestrator.ws.support.EngineRows;
import org.gbif.orchestrator.ws.support.FakeMessages;
import org.gbif.orchestrator.ws.support.RecordingEmails;
import org.gbif.orchestrator.ws.support.TestBeans;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@Import(TestBeans.class)
class JobsTest {

  private static final ObjectMapper JSON = new ObjectMapper();

  @Autowired ClbValidationPoller poller;
  @Autowired RetentionPolicyTask retention;
  @Autowired ValidationRepository validations;
  @Autowired TestBeans.FakeChecklistbank checklistbank;
  @Autowired FakeMessages messages;
  @Autowired RecordingEmails emails;
  @Autowired JdbcClient jdbc;

  @BeforeEach
  void setUp() {
    EngineRows.clear(jdbc);
    messages.clear();
    emails.sent.clear();
    checklistbank.imports.clear();
  }

  @Test
  void retentionCoversEveryOldValidationNotOnlyTwenty() {
    for (int i = 0; i < 25; i++) {
      stored(Status.FINISHED, null, Duration.ofDays(120));
    }
    stored(Status.FINISHED, null, Duration.ofDays(1));

    retention.cleanOutdatedData();

    assertEquals(25, messages.legacy.size());
    PipelinesCleanerMessage first = (PipelinesCleanerMessage) messages.legacy.get(0);
    org.junit.jupiter.api.Assertions.assertTrue(first.isValidator());
    assertEquals(1, first.getAttempt());
  }

  @Test
  void pollerContinuesFinishedImportsAndEndsTheOthers() throws Exception {
    UUID finished = stored(Status.WAITING_FOR_CHECKLISTBANK, 1, Duration.ofDays(2));
    UUID canceled = stored(Status.WAITING_FOR_CHECKLISTBANK, 2, Duration.ofDays(2));
    UUID unreachable = stored(Status.WAITING_FOR_CHECKLISTBANK, 3, Duration.ofDays(2));
    UUID recent = stored(Status.WAITING_FOR_CHECKLISTBANK, 4, Duration.ofHours(1));
    checklistbank.imports.put(1, JSON.readTree("{\"datasetKey\":1,\"status\":\"finished\"}"));
    checklistbank.imports.put(2, JSON.readTree("{\"datasetKey\":2,\"status\":\"canceled\"}"));

    poller.poll();

    assertEquals(Status.FINISHED, status(finished));
    assertEquals(Status.ABORTED, status(canceled));
    assertEquals(Status.FAILED, status(unreachable));
    assertEquals(Status.WAITING_FOR_CHECKLISTBANK, status(recent));

    PipelinesChecklistValidatorMessage next =
        (PipelinesChecklistValidatorMessage) messages.legacy.get(0);
    assertEquals(finished, next.getValidationKey());
    assertEquals("finished", JSON.readTree(next.getClBResponsePayload()).get("status").asText());
    assertEquals(3, emails.sent.size());
  }

  @Test
  void pollerCoversEveryWaitingValidation() throws Exception {
    for (int i = 0; i < 25; i++) {
      stored(Status.WAITING_FOR_CHECKLISTBANK, i, Duration.ofDays(2));
      checklistbank.imports.put(i, JSON.readTree("{\"status\":\"canceled\"}"));
    }

    poller.poll();

    assertEquals(25, emails.sent.size());
  }

  private Status status(UUID key) {
    return validations.get(key).orElseThrow().getStatus();
  }

  private UUID stored(Status status, Integer clbDatasetKey, Duration age) {
    UUID key = UUID.randomUUID();
    validations.create(
        Validation.builder()
            .key(key)
            .username("alice")
            .file("data.zip")
            .status(status)
            .fileFormat(FileFormat.COLDP)
            .build());
    if (clbDatasetKey != null) {
      validations.update(
          Validation.builder().key(key).status(status).clbDatasetKey(clbDatasetKey).build());
    }
    jdbc.sql("UPDATE validation SET created = :created WHERE key = :key")
        .param("created", Timestamp.from(Instant.now().minus(age)))
        .param("key", key)
        .update();
    return key;
  }
}
