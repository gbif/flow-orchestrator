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
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Requests of the DwC-A validation chain (archive check, ChecklistBank wait, DwC-A to verbatim). */
class DwcaRequestFactoriesTest {

  private static final UUID WORKFLOW = UUID.fromString("0b6c1b5e-6a4c-4d7e-9a43-6a0f4c1f7d10");
  private static final UUID DATASET = UUID.fromString("673f2308-3501-474f-85d5-a832825bb46c");
  // Postal's own mapper settings, as the workers read the payload.
  private static final ObjectMapper POSTAL = new ObjectMapper();

  private final ContractJson json = new ContractJson();
  private final PipelinesValidatorRequestFactory pipelines =
      new PipelinesValidatorRequestFactory(json);

  @Test
  void archiveValidatorRequestGoesStraightToTheValidatorWithTheFileFormat() throws Exception {
    StepContext context =
        context(StepType.VALIDATOR_VALIDATE_ARCHIVE, Map.of("fileFormat", "DWCA"), List.of());

    Object request = new ArchiveValidatorRequestFactory(json).create(context);
    PipelinesArchiveValidatorMessage message =
        POSTAL.readValue(json.write(request), PipelinesArchiveValidatorMessage.class);

    assertEquals(DATASET, message.getDatasetUuid());
    assertEquals(1, message.getAttempt());
    assertEquals("DWCA", message.getFileFormat());
    assertEquals(Set.of("VALIDATOR_VALIDATE_ARCHIVE"), message.getPipelineSteps());
    // The routing key the archive validator binds (deployment config sends it there).
    assertEquals("occurrence.pipelines.archive.validator.validator", message.getRoutingKey());
    assertEquals(header(StepType.VALIDATOR_VALIDATE_ARCHIVE), toHeader(message.getStepHeader()));
  }

  @Test
  void archiveValidatorNeedsTheFileFormatParameter() {
    StepContext context = context(StepType.VALIDATOR_VALIDATE_ARCHIVE, Map.of(), List.of());

    assertThrows(
        IllegalStateException.class, () -> new ArchiveValidatorRequestFactory(json).create(context));
  }

  @Test
  void checklistbankRequestCarriesTheHeaderAndTheSubmittedDataset() {
    StepContext context =
        context(
            StepType.VALIDATOR_CHECKLISTBANK,
            Map.of("fileFormat", "COLDP"),
            List.of(
                finished(
                    StepType.VALIDATOR_VALIDATE_ARCHIVE,
                    1,
                    Map.of(OutputKeys.CLB_DATASET_KEY, "4711", OutputKeys.DATASET_TYPE, "CHECKLIST"))));

    JsonNode request =
        json.read(json.write(new ChecklistbankRequestFactory().create(context)), JsonNode.class);

    assertEquals(4711, request.get("clbDatasetKey").asLong());
    assertEquals(
        header(StepType.VALIDATOR_CHECKLISTBANK),
        json.mapper().convertValue(request.get("header"), StepHeader.class));
  }

  @Test
  void checklistbankNeedsTheSubmittedDataset() {
    StepContext context = context(StepType.VALIDATOR_CHECKLISTBANK, Map.of(), List.of());

    assertThrows(
        StepContext.MissingOutputException.class,
        () -> new ChecklistbankRequestFactory().create(context));
  }

  @Test
  void dwcaToVerbatimTakesTheShapeFromTheArchiveCheckAndHasNoCrawlerReport() throws Exception {
    StepContext context =
        context(
            StepType.VALIDATOR_DWCA_TO_VERBATIM,
            Map.of("fileFormat", "DWCA", "checklist", "false"),
            List.of(
                finished(
                    StepType.VALIDATOR_VALIDATE_ARCHIVE,
                    1,
                    Map.of(OutputKeys.DATASET_TYPE, "SAMPLING_EVENT"))));

    PipelinesBalancerMessage wrapped = (PipelinesBalancerMessage) pipelines.create(context);
    PipelinesDwcaMessage dwca = POSTAL.readValue(wrapped.getPayload(), PipelinesDwcaMessage.class);

    assertEquals("PipelinesDwcaMessage", wrapped.getMessageClass());
    assertEquals(DatasetType.SAMPLING_EVENT, dwca.getDatasetType());
    assertNull(dwca.getValidationReport());
    assertEquals(Set.of("VALIDATOR_DWCA_TO_VERBATIM"), dwca.getPipelineSteps());
    assertEquals(PipelinesDwcaMessage.VALIDATOR_ROUTING_KEY, dwca.getRoutingKey());
    assertEquals(header(StepType.VALIDATOR_DWCA_TO_VERBATIM), toHeader(dwca.getStepHeader()));
  }

  @Test
  void sharedStepsUseTheDwcaConversionOutputs() throws Exception {
    List<StepContext.FinishedStep> finished =
        List.of(
            finished(
                StepType.VALIDATOR_DWCA_TO_VERBATIM,
                2,
                Map.of(
                    OutputKeys.DATASET_TYPE, "OCCURRENCE",
                    OutputKeys.OCCURRENCE_RECORDS, "500",
                    OutputKeys.EVENT_RECORDS, "0",
                    OutputKeys.TRIPLET_VALID, "true",
                    OutputKeys.OCCURRENCE_ID_VALID, "true",
                    OutputKeys.INTERPRET_TYPES, "BASIC, TAXONOMY,MULTIMEDIA")),
            finished(StepType.VALIDATOR_VALIDATE_ARCHIVE, 1, Map.of()));

    PipelinesVerbatimMessage identifier =
        POSTAL.readValue(
            ((PipelinesBalancerMessage)
                    pipelines.create(
                        context(StepType.VALIDATOR_VERBATIM_TO_IDENTIFIER, Map.of(), finished)))
                .getPayload(),
            PipelinesVerbatimMessage.class);
    PipelinesInterpretedMessage metrics =
        POSTAL.readValue(
            ((PipelinesBalancerMessage)
                    pipelines.create(
                        context(StepType.VALIDATOR_COLLECT_METRICS, Map.of(), finished)))
                .getPayload(),
            PipelinesInterpretedMessage.class);

    assertTrue(identifier.getValidationResult().isTripletValid());
    assertTrue(identifier.getValidationResult().isOccurrenceIdValid());
    assertEquals(500L, identifier.getValidationResult().getNumberOfRecords());
    assertEquals(Set.of("BASIC", "TAXONOMY", "MULTIMEDIA"), identifier.getInterpretTypes());
    assertEquals(Set.of("BASIC", "TAXONOMY", "MULTIMEDIA"), metrics.getInterpretTypes());
  }

  @Test
  void aFlagThatIsNotABooleanIsRefused() {
    StepContext context =
        context(
            StepType.VALIDATOR_VERBATIM_TO_IDENTIFIER,
            Map.of(),
            List.of(
                finished(
                    StepType.VALIDATOR_DWCA_TO_VERBATIM,
                    2,
                    Map.of(
                        OutputKeys.DATASET_TYPE, "OCCURRENCE",
                        OutputKeys.OCCURRENCE_RECORDS, "1",
                        OutputKeys.EVENT_RECORDS, "0",
                        OutputKeys.TRIPLET_VALID, "yes"))));

    assertThrows(StepContext.MissingOutputException.class, () -> pipelines.create(context));
  }

  private StepHeader toHeader(Map<String, Object> map) {
    return json.mapper().convertValue(map, StepHeader.class);
  }

  private static StepHeader header(StepType type) {
    return new StepHeader(WORKFLOW, DATASET, 1, type, 1);
  }

  private static StepContext.FinishedStep finished(
      StepType type, int order, Map<String, String> outputs) {
    return new StepContext.FinishedStep(type, order, outputs);
  }

  private static StepContext context(
      StepType type, Map<String, String> parameters, List<StepContext.FinishedStep> finished) {
    Instant now = Instant.parse("2026-11-20T10:00:00Z");
    WorkflowExecution execution =
        new WorkflowExecution(
            WORKFLOW,
            Domain.VALIDATION,
            DATASET,
            1,
            1,
            "validation-dwca",
            "hash",
            parameters,
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
