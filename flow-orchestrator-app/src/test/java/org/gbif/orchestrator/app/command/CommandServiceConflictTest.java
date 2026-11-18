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
package org.gbif.orchestrator.app.command;

import org.gbif.orchestrator.api.command.Domain;
import org.gbif.orchestrator.app.persistence.OptimisticConflictException;
import org.gbif.orchestrator.app.support.ConflictingExecutionRepository;
import org.gbif.orchestrator.app.support.Database;

import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Optimistic conflicts roll back and are retried from a fresh read. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Import(ConflictingExecutionRepository.Configuration.class)
class CommandServiceConflictTest {

  @Autowired JdbcClient jdbc;
  @Autowired CommandService service;
  @Autowired ConflictingExecutionRepository conflicting;

  @BeforeEach
  void clear() {
    Database.clear(jdbc);
    conflicting.reset(0);
  }

  @Test
  void conflictIsRetriedAndLeavesNoDuplicates() {
    conflicting.reset(1);

    CommandResult result =
        service.handle(CommandServiceTest.start(Domain.VALIDATION, Map.of("fileFormat", "DWCDP")));

    assertEquals(CommandResult.Kind.ACCEPTED, result.kind());
    assertEquals(2, conflicting.calls(), "first try conflicted, second succeeded");
    assertEquals(1, Database.count(jdbc, "workflow_execution"));
    assertEquals(3, Database.count(jdbc, "step_execution"));
    assertEquals(1, Database.count(jdbc, "step_attempt"));
    assertEquals(1, Database.count(jdbc, "outbox_message"));
    assertEquals(1, Database.count(jdbc, "workflow_command"));
  }

  @Test
  void persistentConflictGivesUpAfterMaxTriesAndWritesNothing() {
    conflicting.reset(CommandService.MAX_TRIES);

    assertThrows(
        OptimisticConflictException.class,
        () ->
            service.handle(
                CommandServiceTest.start(Domain.VALIDATION, Map.of("fileFormat", "DWCDP"))));

    assertEquals(CommandService.MAX_TRIES, conflicting.calls());
    assertEquals(0, Database.count(jdbc, "workflow_execution"));
    assertEquals(0, Database.count(jdbc, "outbox_message"));
    assertEquals(0, Database.count(jdbc, "workflow_command"));
  }

}
