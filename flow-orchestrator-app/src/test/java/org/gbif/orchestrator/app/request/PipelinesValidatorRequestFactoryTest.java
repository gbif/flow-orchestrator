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
package org.gbif.orchestrator.app.request;

import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.api.messages.DwcDpStageMessage;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.orchestrator.api.command.Domain;
import org.gbif.orchestrator.api.step.OutputKeys;
import org.gbif.orchestrator.api.step.StepHeader;
import org.gbif.orchestrator.app.json.ContractJson;
import org.gbif.orchestrator.app.persistence.WorkflowExecution;
import org.gbif.orchestrator.engine.WorkflowState.Status;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PipelinesValidatorRequestFactoryTest {

  private static final UUID WORKFLOW = UUID.fromString("0b6c1b5e-6a4c-4d7e-9a43-6a0f4c1f7d10");
  private static final UUID DATASET = UUID.fromString("673f2308-3501-474f-85d5-a832825bb46c");
  // Postal's own mapper settings, as the balancer reads the payload.
  private static final ObjectMapper POSTAL = new ObjectMapper();

  private final ContractJson json = new ContractJson();
  private final PipelinesValidatorRequestFactory factory =
      new PipelinesValidatorRequestFactory(json);

  @Test
  void stageRequestCarriesHeaderAndOnlyItsOwnStep() throws Exception {
    StepContext context = context(StepType.VALIDATOR_DWCDP_STAGE, List.of());

    PipelinesBalancerMessage wrapped = (PipelinesBalancerMessage) factory.create(context);

    assertEquals("DwcDpStageMessage", wrapped.getMessageClass());
    DwcDpStageMessage stage = POSTAL.readValue(wrapped.getPayload(), DwcDpStageMessage.class);
    assertEquals(DATASET, stage.getDatasetUuid());
    assertEquals(2, stage.getAttempt());
    assertEquals(Set.of("VALIDATOR_DWCDP_STAGE"), stage.getPipelineSteps());
    assertFalse(stage.isContainsEvents());
    assertFalse(stage.isContainsOccurrences());
    assertEquals(header(StepType.VALIDATOR_DWCDP_STAGE), toHeader(stage.getStepHeader()));
  }

  @Test
  void identifierRequestUsesConversionOutputsAndRoutesToIdentifierQueue() throws Exception {
    StepContext context =
        context(
            StepType.VALIDATOR_VERBATIM_TO_IDENTIFIER,
            List.of(
                finished(
                    StepType.VALIDATOR_DWCDP_TO_VERBATIM,
                    2,
                    Map.of(
                        OutputKeys.DATASET_TYPE, "SAMPLING_EVENT",
                        OutputKeys.OCCURRENCE_RECORDS, "120",
                        OutputKeys.EVENT_RECORDS, "7")),
                finished(StepType.VALIDATOR_DWCDP_STAGE, 1, Map.of())));

    PipelinesBalancerMessage wrapped = (PipelinesBalancerMessage) factory.create(context);
    PipelinesVerbatimMessage verbatim =
        POSTAL.readValue(wrapped.getPayload(), PipelinesVerbatimMessage.class);

    assertEquals("PipelinesVerbatimMessage", wrapped.getMessageClass());
    assertEquals(DatasetType.SAMPLING_EVENT, verbatim.getDatasetType());
    assertEquals(120L, verbatim.getValidationResult().getNumberOfRecords());
    assertEquals(7L, verbatim.getValidationResult().getNumberOfEventRecords());
    assertEquals(Set.of("ALL"), verbatim.getInterpretTypes());
    assertEquals(
        "occurrence.pipelines.verbatim.finished.validator.identifier",
        verbatim.getRoutingKey().replaceAll("\\.(standalone|distributed)$", ""));
    assertEquals(
        header(StepType.VALIDATOR_VERBATIM_TO_IDENTIFIER), toHeader(verbatim.getStepHeader()));
  }

  @Test
  void metricsRequestCarriesCountsAndHeader() throws Exception {
    StepContext context =
        context(
            StepType.VALIDATOR_COLLECT_METRICS,
            List.of(
                finished(StepType.VALIDATOR_VERBATIM_TO_INTERPRETED, 4, Map.of()),
                finished(
                    StepType.VALIDATOR_DWCDP_TO_VERBATIM,
                    2,
                    Map.of(
                        OutputKeys.DATASET_TYPE, "OCCURRENCE",
                        OutputKeys.OCCURRENCE_RECORDS, "120",
                        OutputKeys.EVENT_RECORDS, "0"))));

    PipelinesBalancerMessage wrapped = (PipelinesBalancerMessage) factory.create(context);
    PipelinesInterpretedMessage interpreted =
        POSTAL.readValue(wrapped.getPayload(), PipelinesInterpretedMessage.class);

    assertEquals(120L, interpreted.getNumberOfRecords());
    assertEquals(Set.of("VALIDATOR_COLLECT_METRICS"), interpreted.getPipelineSteps());
    assertEquals(
        header(StepType.VALIDATOR_COLLECT_METRICS), toHeader(interpreted.getStepHeader()));
  }

  @Test
  void latestFinishedStepWinsForAnOutput() {
    StepContext context =
        context(
            StepType.VALIDATOR_COLLECT_METRICS,
            List.of(
                finished(StepType.VALIDATOR_VERBATIM_TO_INTERPRETED, 4, Map.of("n", "late")),
                finished(StepType.VALIDATOR_DWCDP_TO_VERBATIM, 2, Map.of("n", "early"))));

    assertEquals("late", context.requireOutput("n"));
  }

  @Test
  void missingOutputIsReportedWithTheStepThatNeedsIt() {
    StepContext context = context(StepType.VALIDATOR_VERBATIM_TO_INTERPRETED, List.of());

    StepContext.MissingOutputException e =
        assertThrows(StepContext.MissingOutputException.class, () -> factory.create(context));
    assertEquals(true, e.getMessage().contains("VALIDATOR_VERBATIM_TO_INTERPRETED"));
  }

  @Test
  void analysisRequestKeepsTheAnalysersContract() {
    StepContext context = context(StepType.VALIDATOR_VALIDATE_DWCDP, List.of());

    JsonNode request =
        json.read(json.write(new DwcDpAnalysisRequestFactory().create(context)), JsonNode.class);

    assertEquals(2, request.size());
    assertEquals(DATASET.toString(), request.get("datasetUuid").asText());
    assertEquals(2, request.get("attempt").asInt());
  }

  @Test
  void stepWithoutFactoryGetsHeaderOnlyRequest() {
    StepRequests requests = new StepRequests(List.of(factory));
    StepContext context = context(StepType.VALIDATOR_UPLOAD_ARCHIVE, List.of());

    JsonNode request = json.read(json.write(requests.create(context)), JsonNode.class);

    assertEquals(
        header(StepType.VALIDATOR_UPLOAD_ARCHIVE),
        json.mapper().convertValue(request.get("header"), StepHeader.class));
  }

  private StepHeader toHeader(Map<String, Object> map) {
    return json.mapper().convertValue(map, StepHeader.class);
  }

  private static StepHeader header(StepType type) {
    return new StepHeader(WORKFLOW, DATASET, 2, type, 1);
  }

  private static StepContext.FinishedStep finished(
      StepType type, int order, Map<String, String> outputs) {
    return new StepContext.FinishedStep(type, order, outputs);
  }

  private static StepContext context(StepType type, List<StepContext.FinishedStep> finished) {
    Instant now = Instant.parse("2026-11-18T10:00:00Z");
    WorkflowExecution execution =
        new WorkflowExecution(
            WORKFLOW,
            Domain.VALIDATION,
            DATASET,
            2,
            1,
            "validation-dwcdp",
            "hash",
            Map.of("fileFormat", "DWCDP"),
            Status.RUNNING,
            1,
            now,
            now,
            null,
            false,
            null);
    return new StepContext(header(type), execution, finished);
  }
}
