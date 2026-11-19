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
package org.gbif.orchestrator.ws.jobs;

import org.gbif.common.messaging.api.messages.PipelinesCleanerMessage;
import org.gbif.orchestrator.ws.api.Validation;
import org.gbif.orchestrator.ws.api.ValidationSearchRequest;
import org.gbif.orchestrator.ws.messaging.Messages;
import org.gbif.orchestrator.ws.persistence.ValidationRepository;

import java.io.IOException;
import java.time.Clock;
import java.time.LocalDate;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;

/**
 * Sends the validator cleaner message for validations older than {@code retentionPolicy.keepDays}
 * (counted from the start of today). The pipelines cleaner deletes their files and then deletes
 * the validation through this API.
 *
 * <p>Every due validation is handled (validator-ws only took the first 20 per run).
 */
@Component
public class RetentionPolicyTask {

  private static final Logger LOG = LoggerFactory.getLogger(RetentionPolicyTask.class);

  private final ValidationRepository validations;
  private final Messages messages;
  private final Clock clock;
  private final int keepDays;

  public RetentionPolicyTask(
      ValidationRepository validations,
      Messages messages,
      Clock clock,
      @Value("${retentionPolicy.keepDays}") int keepDays) {
    this.validations = validations;
    this.messages = messages;
    this.clock = clock;
    this.keepDays = keepDays;
  }

  @Scheduled(cron = "${retentionPolicy.cron}")
  @SchedulerLock(name = "retentionPolicy")
  public void cleanOutdatedData() {
    Date createdBefore =
        Date.from(
            LocalDate.now(clock).minusDays(keepDays).atStartOfDay(clock.getZone()).toInstant());
    List<Validation> due =
        validations.list(
            null, ValidationSearchRequest.builder().toDate(createdBefore).limit(null).build());
    LOG.info("Retention: {} validations created before {}", due.size(), createdBefore);
    for (Validation validation : due) {
      PipelinesCleanerMessage message = new PipelinesCleanerMessage();
      message.setValidator(true);
      message.setDatasetUuid(validation.getKey());
      message.setAttempt(1);
      try {
        messages.send(message);
      } catch (IOException e) {
        LOG.error("Could not send the cleaner message for validation {}", validation.getKey(), e);
      }
    }
  }
}
