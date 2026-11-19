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

import org.gbif.eml.validation.EmlIssue;
import org.gbif.eml.validation.EmlSchema;
import org.gbif.eml.validation.EmlValidator;
import org.gbif.orchestrator.ws.api.EvaluationType;
import org.gbif.orchestrator.ws.api.Metrics.IssueInfo;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "EML Validation", description = "EML validation operations")
@RestController
@RequestMapping(value = "validation/eml", produces = MediaType.APPLICATION_JSON_VALUE)
public class EmlValidationResource {

  /**
   * Schema validation only, as before (the GBIF quality rules are not part of this endpoint). A
   * document without a usable schema location is validated against the current GBIF profile.
   */
  private static final EmlValidator VALIDATOR =
      EmlValidator.builder().defaultSchema(EmlSchema.GBIF_PROFILE_1_3).qualityRules(false).build();

  /** Validates an EML document against the bundled schemas (gbif-eml-validation). */
  @Operation(
      summary = "Validate EML document",
      description = "Validates an EML XML document against supported schemas.")
  @ApiResponses({
    @ApiResponse(
        responseCode = "200",
        description = "Validation issues returned",
        content = @Content(schema = @Schema(implementation = IssueInfo.class))),
    @ApiResponse(responseCode = "400", description = "Invalid EML document")
  })
  @PostMapping(
      consumes = {MediaType.APPLICATION_XML_VALUE},
      produces = {MediaType.APPLICATION_JSON_VALUE})
  public List<IssueInfo> validateEml(
      @io.swagger.v3.oas.annotations.parameters.RequestBody(
              description = "EML document as XML bytes")
          @RequestBody
          byte[] document) {
    return VALIDATOR.validate(document).issues().stream()
        .map(EmlValidationResource::toIssueInfo)
        .collect(Collectors.toList());
  }

  /** The supported (bundled) schema locations. */
  @Operation(
      summary = "List supported EML schemas",
      description = "Returns the supported EML schema locations.")
  @ApiResponses({
    @ApiResponse(
        responseCode = "200",
        description = "Schemas returned",
        content = @Content(schema = @Schema(implementation = String.class)))
  })
  @GetMapping("schemas")
  public List<String> getSchemas() {
    return Arrays.stream(EmlSchema.values()).map(EmlSchema::location).toList();
  }

  /**
   * In the shape validator-ws returned: issue {@code EML_GBIF_SCHEMA}, with the severity as the key
   * of the sample's related data and the parser's message (with its position) as the value.
   */
  static IssueInfo toIssueInfo(EmlIssue issue) {
    String message =
        issue.line() == null
            ? issue.message()
            : "line " + issue.line() + ", column " + issue.column() + ": " + issue.message();
    return IssueInfo.create(EvaluationType.EML_GBIF_SCHEMA, issue.severity().name(), message);
  }
}
