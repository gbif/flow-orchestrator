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
package org.gbif.orchestrator.ws.resource;

import org.gbif.api.vocabulary.UserRole;
import org.gbif.orchestrator.api.report.ReportTypes;
import org.gbif.orchestrator.ws.api.FileFormat;
import org.gbif.orchestrator.ws.api.Validation;
import org.gbif.orchestrator.ws.api.Validation.Status;
import org.gbif.orchestrator.ws.persistence.ValidationRepository;
import org.gbif.orchestrator.ws.support.Archives;
import org.gbif.orchestrator.ws.support.EngineRows;
import org.gbif.orchestrator.ws.support.FakeMessages;
import org.gbif.orchestrator.ws.support.TestBeans;
import org.gbif.orchestrator.ws.support.Users;

import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.security.core.Authentication;
import org.springframework.test.web.servlet.MockMvc;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.authentication;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** The HTTP API in today's shape, through the security filters and JSON serialization. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@AutoConfigureMockMvc
@Import(TestBeans.class)
class ValidationResourceTest {

  private static final Authentication ALICE = Users.authentication("alice", UserRole.USER);
  private static final Authentication MALLORY = Users.authentication("mallory", UserRole.USER);

  @Autowired MockMvc mvc;
  @Autowired ValidationRepository validations;
  @Autowired FakeMessages messages;
  @Autowired JdbcClient jdbc;

  @BeforeEach
  void setUp() {
    EngineRows.clear(jdbc);
    messages.clear();
  }

  @Test
  void uploadAnswersWithTheSubmittedValidation() throws Exception {
    // gbif-common-ws answers POSTs with 201 and void responses with 204, as for validator-ws
    mvc.perform(multipart("/validation").file(Archives.dwcDp("pumas.zip")).with(authentication(ALICE)))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.status").value("SUBMITTED"))
        .andExpect(jsonPath("$.username").value("alice"))
        .andExpect(jsonPath("$.file").value("pumas.zip"));
  }

  @Test
  void uploadNeedsAUser() throws Exception {
    mvc.perform(multipart("/validation").file(Archives.dwcDp("pumas.zip")))
        .andExpect(status().is4xxClientError());
  }

  @Test
  void aValidationReadsAsBefore() throws Exception {
    UUID key = stored("alice", Status.QUEUED, FileFormat.DWCDP);
    jdbc.sql("UPDATE validation SET notification_emails = '[\"ipt@example.org\"]' WHERE key = :k")
        .param("k", key)
        .update();
    UUID run = EngineRows.execution(jdbc, key, "RUNNING", false, null);
    EngineRows.step(jdbc, run, "VALIDATOR_VALIDATE_DWCDP", 0, "RUNNING", null);

    mvc.perform(get("/validation/{key}", key).with(authentication(MALLORY)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.key").value(key.toString()))
        .andExpect(jsonPath("$.status").value("RUNNING"))
        .andExpect(jsonPath("$.fileFormat").value("DWCDP"))
        .andExpect(jsonPath("$.metrics.stepTypes[1].stepType").value("VALIDATOR_VALIDATE_DWCDP"))
        .andExpect(jsonPath("$.metrics.stepTypes[1].status").value("RUNNING"))
        .andExpect(jsonPath("$.metrics.files").isArray())
        .andExpect(jsonPath("$.notificationEmails").doesNotExist())
        .andExpect(jsonPath("$.dataset").doesNotExist());
  }

  @Test
  void anUnknownValidationIs404() throws Exception {
    mvc.perform(get("/validation/{key}", UUID.randomUUID()).with(authentication(ALICE)))
        .andExpect(status().isNotFound());
  }

  @Test
  void theLegacyPutIsRefusedForOrchestratedValidations() throws Exception {
    UUID key = stored("alice", Status.QUEUED, FileFormat.DWCDP);
    mvc.perform(
            put("/validation/{key}", key)
                .with(authentication(ALICE))
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"key\":\"" + key + "\",\"status\":\"FINISHED\"}"))
        .andExpect(status().isConflict());
  }

  @Test
  void othersCannotCancel() throws Exception {
    UUID key = stored("alice", Status.QUEUED, FileFormat.DWCA);
    mvc.perform(put("/validation/{key}/cancel", key).with(authentication(MALLORY)))
        .andExpect(status().isForbidden());
    mvc.perform(put("/validation/{key}/cancel", key).with(authentication(ALICE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.status").value("ABORTED"));
  }

  @Test
  void theChecklistBankCallbackNeedsNoUser() throws Exception {
    UUID key = stored("alice", Status.WAITING_FOR_CHECKLISTBANK, FileFormat.COLDP);
    mvc.perform(
            post("/validation/clbValidationCallback/{key}", key)
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"datasetKey\":7,\"status\":\"finished\"}"))
        .andExpect(status().isNoContent());
    org.junit.jupiter.api.Assertions.assertEquals(1, messages.legacy.size());
  }

  @Test
  void workflowReportsCanBeRead() throws Exception {
    UUID key = stored("alice", Status.QUEUED, FileFormat.DWCDP);
    EngineRows.report(jdbc, key, ReportTypes.DWCDP_ANALYSIS, "{\"metadata\":{\"isValid\":true}}");

    mvc.perform(
            get("/validation/{key}/validationreport/1/DWCDP_ANALYSIS", key)
                .with(authentication(ALICE)))
        .andExpect(status().isOk());
    mvc.perform(
            get("/validation/{key}/validationreport/1/IDENTIFIERS", key)
                .with(authentication(ALICE)))
        .andExpect(status().isNotFound());
  }

  @Test
  void emlIsValidatedWithTheSharedLibrary() throws Exception {
    String eml =
        "<eml:eml xmlns:eml=\"https://eml.ecoinformatics.org/eml-2.2.0\""
            + " xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""
            + " xsi:schemaLocation=\"https://eml.ecoinformatics.org/eml-2.2.0"
            + " https://rs.gbif.org/schema/eml-gbif-profile/1.3/eml.xsd\""
            + " packageId=\"p\" system=\"s\"><dataset><title2/></dataset></eml:eml>";
    mvc.perform(post("/validation/eml").contentType(MediaType.APPLICATION_XML).content(eml))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$[0].issue").value("EML_GBIF_SCHEMA"))
        .andExpect(jsonPath("$[0].issueCategory").value("RESOURCE_STRUCTURE"));
    mvc.perform(get("/validation/eml/schemas"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$").value(hasItem("http://rs.gbif.org/schema/eml-gbif-profile/1.3/eml.xsd")));
  }

  @Test
  void enumerationsAreServed() throws Exception {
    mvc.perform(get("/validation/enumeration/FileFormat"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$").value(hasItem("DWCDP")))
        .andExpect(jsonPath("$").value(not(hasItem("CLB"))));
  }

  private UUID stored(String user, Status status, FileFormat format) {
    UUID key = UUID.randomUUID();
    validations.create(
        Validation.builder()
            .key(key)
            .username(user)
            .file("data.zip")
            .status(status)
            .fileFormat(format)
            .build());
    return key;
  }
}
