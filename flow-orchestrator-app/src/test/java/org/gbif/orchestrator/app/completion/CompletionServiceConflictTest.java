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
package org.gbif.orchestrator.app.completion;

import org.gbif.api.model.pipelines.StepType;
import org.gbif.orchestrator.api.command.CommandType;
import org.gbif.orchestrator.api.command.Domain;
import org.gbif.orchestrator.api.command.WorkflowCommand;
import org.gbif.orchestrator.api.step.Outcome;
import org.gbif.orchestrator.api.step.Report;
import org.gbif.orchestrator.app.command.CommandService;
import org.gbif.orchestrator.app.persistence.OptimisticConflictException;
import org.gbif.orchestrator.app.persistence.RetryingTransactions;
import org.gbif.orchestrator.app.support.ConflictingExecutionRepository;
import org.gbif.orchestrator.app.support.Database;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** A completion that loses an optimistic conflict is re-run from a fresh read. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Import(ConflictingExecutionRepository.Configuration.class)
class CompletionServiceConflictTest {

  @Autowired JdbcClient jdbc;
  @Autowired CommandService commands;
  @Autowired CompletionService service;
  @Autowired ConflictingExecutionRepository conflicting;

  private UUID id;

  @BeforeEach
  void startWorkflow() {
    Database.clear(jdbc);
    conflicting.reset(0);
    id =
        commands
            .handle(
                new WorkflowCommand(
                    UUID.randomUUID(),
                    CommandType.START,
                    Domain.VALIDATION,
                    UUID.fromString("673f2308-3501-474f-85d5-a832825bb46c"),
                    1,
                    null,
                    Map.of("fileFormat", "DWCDP"),
                    "jdoe",
                    null,
                    Instant.now()))
            .workflowExecutionId();
  }

  private static final List<Report> VALID = List.of(CompletionServiceTest.analysis(true, null));

  @Test
  void conflictIsRetriedAndDispatchesOnce() {
    conflicting.reset(1);

    CompletionResult result =
        service.handle(
            CompletionServiceTest.completed(
                id, StepType.VALIDATOR_VALIDATE_DWCDP, 1, Outcome.SUCCEEDED, null, VALID));

    assertEquals(CompletionResult.PROCESSED, result);
    assertEquals(2, conflicting.calls());
    assertEquals(2, Database.count(jdbc, "step_attempt"));
    assertEquals(2, Database.count(jdbc, "outbox_message"));
  }

  @Test
  void persistentConflictRollsBackEverything() {
    conflicting.reset(RetryingTransactions.MAX_TRIES);

    assertThrows(
        OptimisticConflictException.class,
        () ->
            service.handle(
                CompletionServiceTest.completed(
                    id,
                    StepType.VALIDATOR_VALIDATE_DWCDP,
                    1,
                    Outcome.SUCCEEDED,
                    null,
                    VALID)));

    assertEquals(1, Database.count(jdbc, "step_attempt"));
    assertEquals(1, Database.count(jdbc, "outbox_message"));
    assertEquals(0, Database.count(jdbc, "workflow_report"));
  }
}
