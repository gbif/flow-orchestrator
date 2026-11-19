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
package org.gbif.orchestrator.ws.service;

import org.gbif.api.model.pipelines.PipelinesWorkflow;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.orchestrator.ws.api.Metrics;
import org.gbif.orchestrator.ws.api.Metrics.ValidationStep;
import org.gbif.orchestrator.ws.api.Validation;
import org.gbif.orchestrator.ws.api.Validation.Status;
import org.gbif.orchestrator.ws.file.DataFile;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import lombok.experimental.UtilityClass;

/** Utility class to create instances of API classes. */
@UtilityClass
public class ValidationFactory {

  static Validation newValidationInstance(
      UUID key,
      DataFile dataFile,
      String userName,
      Validation.Status status,
      String sourceId,
      UUID installationKey,
      Set<String> notificationEmails) {
    return Validation.builder()
        .key(key)
        .fileFormat(dataFile.getFileFormat())
        .status(status)
        .file(dataFile.getFilePath().getFileName().toString())
        .fileSize(dataFile.getSize())
        .username(userName)
        .installationKey(installationKey)
        .sourceId(sourceId)
        .notificationEmails(notificationEmails)
        .metrics(
            Metrics.builder()
                .stepTypes(
                    Collections.singletonList(
                        ValidationStep.builder()
                            .stepType(StepType.VALIDATOR_UPLOAD_ARCHIVE.name())
                            .status(Status.RUNNING)
                            .executionOrder(
                                PipelinesWorkflow.getValidatorWorkflow()
                                    .getLevel(StepType.VALIDATOR_UPLOAD_ARCHIVE))
                            .build()))
                .build())
        .build();
  }

  static Validation newValidationInstance(UUID key, DataFile dataFile, Validation.Status status) {
    return Validation.builder()
        .key(key)
        .fileFormat(dataFile.getFileFormat())
        .status(status)
        .file(dataFile.getFilePath().getFileName().toString())
        .fileSize(dataFile.getSize())
        .build();
  }

  static Validation newValidationInstance(UUID key, Validation.Status status, Metrics metrics) {
    return Validation.builder().key(key).status(status).metrics(metrics).build();
  }

  static Metrics metricsSubmitError(String errorMessage) {
    return Metrics.builder()
        .stepTypes(StepsMapper.getUploadingSteps(Status.FAILED, errorMessage))
        .build();
  }
}
