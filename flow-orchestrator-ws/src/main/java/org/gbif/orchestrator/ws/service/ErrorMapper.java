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

import org.gbif.orchestrator.ws.api.Validation.ErrorCode;

import java.util.function.Function;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

import lombok.RequiredArgsConstructor;

/** Maps Error responses to ResponseStatusException. */
@Component
@RequiredArgsConstructor
public class ErrorMapper implements Function<ErrorCode, ResponseStatusException> {

  @Value("${maxRunningValidationPerUser}")
  private final int maxRunningValidationPerUser;

  @Override
  public ResponseStatusException apply(ErrorCode error) {
    HttpStatus status = HttpStatus.INTERNAL_SERVER_ERROR;
    String message = "Error processing request";

    switch (error) {
      case MAX_RUNNING_VALIDATIONS:
        status = HttpStatus.METHOD_FAILURE;
        message =
            "Maximum number of executing validations of "
                + maxRunningValidationPerUser
                + " reached\n";
        break;
      case MAX_FILE_SIZE_VIOLATION:
        status = HttpStatus.BAD_REQUEST;
        message = "File exceeds the maximum size allowed";
        break;
      case VALIDATION_IS_NOT_EXECUTING:
        status = HttpStatus.BAD_REQUEST;
        message = "Validation is not in executing state";
        break;
      case NOTIFICATION_EMAILS_MISSING:
        status = HttpStatus.BAD_REQUEST;
        message = "Notification emails must be provided when installation key is present";
        break;
      case AUTHORIZATION_ERROR:
        status = HttpStatus.FORBIDDEN;
        message = "Not allowed to modify this validation";
        break;
      case NOT_FOUND:
        status = HttpStatus.NOT_FOUND;
        message = "Validation not found";
        break;
      case ORCHESTRATED:
        status = HttpStatus.CONFLICT;
        message = "This validation runs in the orchestrator; its status can't be set";
        break;
      case WRONG_KEY_IN_REQUEST:
        status = HttpStatus.BAD_REQUEST;
        message = "Wrong validation key for this url";
        break;
      default:
        break;
    }
    return new ResponseStatusException(status, message);
  }
}
