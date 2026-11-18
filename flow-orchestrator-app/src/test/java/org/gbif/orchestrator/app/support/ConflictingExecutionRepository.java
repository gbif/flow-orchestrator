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

import org.gbif.orchestrator.app.json.ContractJson;
import org.gbif.orchestrator.app.persistence.WorkflowExecutionRepository;
import org.gbif.orchestrator.engine.WorkflowState.Status;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * Simulates another transaction changing the workflow between read and write: bumps the version
 * just before the conditional update, for the configured number of calls. Import {@link
 * Configuration} to replace the real repository.
 */
public class ConflictingExecutionRepository extends WorkflowExecutionRepository {

  private final AtomicInteger conflicts = new AtomicInteger();
  private final AtomicInteger calls = new AtomicInteger();
  private final JdbcClient jdbc;

  public ConflictingExecutionRepository(JdbcClient jdbc, ContractJson json) {
    super(jdbc, json);
    this.jdbc = jdbc;
  }

  // Accessed through methods: the bean is proxied (@Repository exception translation).
  public void reset(int conflictCount) {
    conflicts.set(conflictCount);
    calls.set(0);
  }

  public int calls() {
    return calls.get();
  }

  @Override
  public boolean updateIfVersion(
      UUID id,
      int expectedVersion,
      Status status,
      Instant modified,
      Instant finished,
      boolean rejected,
      String reason) {
    calls.incrementAndGet();
    if (conflicts.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
      jdbc.sql("UPDATE workflow_execution SET version = version + 1 WHERE id = :id")
          .param("id", id)
          .update();
    }
    return super.updateIfVersion(
        id, expectedVersion, status, modified, finished, rejected, reason);
  }

  /** Replaces the execution repository with the conflicting one. */
  @TestConfiguration(proxyBeanMethods = false)
  public static class Configuration {
    @Bean
    @Primary
    ConflictingExecutionRepository conflictingExecutionRepository(
        JdbcClient jdbc, ContractJson json) {
      return new ConflictingExecutionRepository(jdbc, json);
    }
  }
}
