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
package org.gbif.orchestrator.ws.persistence;

import org.gbif.orchestrator.ws.api.FileFormat;
import org.gbif.orchestrator.ws.api.Metrics;
import org.gbif.orchestrator.ws.api.Validation;
import org.gbif.orchestrator.ws.api.Validation.Status;
import org.gbif.orchestrator.ws.api.ValidationSearchRequest;
import org.gbif.orchestrator.ws.support.EngineRows;
import org.gbif.orchestrator.ws.support.TestBeans;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@Import(TestBeans.class)
class ValidationRepositoryTest {

  @Autowired ValidationRepository repository;
  @Autowired JdbcClient jdbc;

  @BeforeEach
  void clear() {
    EngineRows.clear(jdbc);
  }

  @Test
  void roundTripKeepsEverything() {
    UUID key = UUID.randomUUID();
    Validation v =
        Validation.builder()
            .key(key)
            .username("alice")
            .file("a.zip")
            .fileSize(10L)
            .status(Status.QUEUED)
            .fileFormat(FileFormat.DWCDP)
            .notificationEmails(Set.of("ipt@example.org"))
            .installationKey(UUID.randomUUID())
            .sourceId("src")
            .build();
    v.getMetrics().setError("boom");
    repository.create(v);

    Validation read = repository.get(key).orElseThrow();
    assertEquals(Set.of("ipt@example.org"), read.getNotificationEmails());
    assertEquals(FileFormat.DWCDP, read.getFileFormat());
    assertEquals("boom", read.getMetrics().getError());
    assertEquals("src", read.getSourceId());
    assertTrue(read.getCreated() != null);
  }

  @Test
  void updatesKeepWhatTheyDoNotSet() {
    UUID key = created("alice", Status.RUNNING, "2026-01-01T00:00:00Z");
    repository.update(Validation.builder().key(key).status(Status.FINISHED).metrics(null).build());

    Validation read = repository.get(key).orElseThrow();
    assertEquals(Status.FINISHED, read.getStatus());
    assertEquals("a.zip", read.getFile());
    assertEquals(FileFormat.DWCA, read.getFileFormat());
  }

  @Test
  void onlyExecutingValidationsFinishOrUpdateIfExecuting() {
    UUID key = created("alice", Status.QUEUED, "2026-01-01T00:00:00Z");
    assertTrue(repository.finish(key, Status.FINISHED, Metrics.builder().build()));
    assertFalse(repository.finish(key, Status.FAILED, Metrics.builder().build()));
    assertEquals(
        0, repository.updateIfExecuting(Validation.builder().key(key).status(Status.FAILED).build()));
    assertEquals(Status.FINISHED, repository.get(key).orElseThrow().getStatus());
  }

  @Test
  void listingFiltersSortsNewestFirstAndPages() {
    UUID old = created("alice", Status.FINISHED, "2026-01-01T00:00:00Z");
    UUID mid = created("alice", Status.RUNNING, "2026-02-01T00:00:00Z");
    UUID recent = created("alice", Status.FINISHED, "2026-03-01T00:00:00Z");
    created("bob", Status.FINISHED, "2026-03-02T00:00:00Z");
    UUID deleted = created("alice", Status.FINISHED, "2026-03-03T00:00:00Z");
    repository.delete(deleted);

    ValidationSearchRequest all = ValidationSearchRequest.builder().build();
    assertEquals(List.of(recent, mid, old), keys(repository.list("alice", all)));
    assertEquals(3, repository.count("alice", all));

    ValidationSearchRequest finished =
        ValidationSearchRequest.builder().status(Set.of(Status.FINISHED)).build();
    assertEquals(List.of(recent, old), keys(repository.list("alice", finished)));

    ValidationSearchRequest page = ValidationSearchRequest.builder().offset(1L).limit(1).build();
    assertEquals(List.of(mid), keys(repository.list("alice", page)));

    ValidationSearchRequest ascending =
        ValidationSearchRequest.builder().sortByCreated(ValidationSearchRequest.SortOrder.ASC).build();
    assertEquals(List.of(old, mid, recent), keys(repository.list("alice", ascending)));

    assertEquals(4, repository.count(null, all));
    assertTrue(repository.get(deleted).orElseThrow().getDeleted() != null);
  }

  private static List<UUID> keys(List<Validation> validations) {
    return validations.stream().map(Validation::getKey).toList();
  }

  private UUID created(String user, Status status, String created) {
    UUID key = UUID.randomUUID();
    repository.create(
        Validation.builder()
            .key(key)
            .username(user)
            .file("a.zip")
            .status(status)
            .fileFormat(FileFormat.DWCA)
            .build());
    jdbc.sql("UPDATE validation SET created = :created WHERE key = :key")
        .param("created", Timestamp.from(Instant.parse(created)))
        .param("key", key)
        .update();
    return key;
  }
}
