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

import org.gbif.common.messaging.api.messages.PipelinesChecklistValidatorMessage;
import org.gbif.orchestrator.ws.api.Validation;
import org.gbif.orchestrator.ws.api.Validation.Status;
import org.gbif.orchestrator.ws.api.ValidationSearchRequest;
import org.gbif.orchestrator.ws.clb.ChecklistbankClient;
import org.gbif.orchestrator.ws.messaging.Messages;
import org.gbif.orchestrator.ws.persistence.ValidationRepository;
import org.gbif.orchestrator.ws.service.ValidationServiceImpl;

import java.time.Clock;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;

import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;

/**
 * Safety net for ChecklistBank validations (formerly {@code ClbValidationCleaner}): asks
 * ChecklistBank about validations that have waited longer than {@code clb.cleaner.hoursOld}, in
 * case its callback was lost. A finished import continues like the callback; a canceled one ends
 * ABORTED; anything else, or an error, ends FAILED. Until Round B redesigns the wait.
 *
 * <p>Every due validation is handled (validator-ws only took the first 20 per run).
 */
@Component
public class ClbValidationPoller {

  private static final Logger LOG = LoggerFactory.getLogger(ClbValidationPoller.class);

  private final ValidationRepository validations;
  private final ValidationServiceImpl service;
  private final ChecklistbankClient checklistbank;
  private final Messages messages;
  private final Clock clock;
  private final int hoursOld;

  public ClbValidationPoller(
      ValidationRepository validations,
      ValidationServiceImpl service,
      ChecklistbankClient checklistbank,
      Messages messages,
      Clock clock,
      @Value("${clb.cleaner.hoursOld}") int hoursOld) {
    this.validations = validations;
    this.service = service;
    this.checklistbank = checklistbank;
    this.messages = messages;
    this.clock = clock;
    this.hoursOld = hoursOld;
  }

  @Scheduled(cron = "${clb.cleaner.cron}")
  @SchedulerLock(name = "clbValidationPoller")
  public void poll() {
    Date createdBefore = Date.from(clock.instant().minus(Duration.ofHours(hoursOld)));
    List<Validation> waiting =
        validations.list(
            null,
            ValidationSearchRequest.builder()
                .status(Set.of(Status.WAITING_FOR_CHECKLISTBANK))
                .toDate(createdBefore)
                .limit(null)
                .build());
    LOG.info(
        "{} validations wait for ChecklistBank since before {}", waiting.size(), createdBefore);
    waiting.stream().filter(v -> v.getClbDatasetKey() != null).forEach(this::check);
  }

  void check(Validation validation) {
    Status status = Status.FAILED;
    try {
      Optional<JsonNode> latest = checklistbank.latestImport(validation.getClbDatasetKey());
      String importStatus = latest.map(ChecklistbankClient::status).orElse(null);
      if (ChecklistbankClient.FINISHED.equalsIgnoreCase(importStatus)) {
        status = Status.FINISHED;
        // continue the process as the callback would have
        messages.send(
            new PipelinesChecklistValidatorMessage(validation.getKey(), latest.get().toString()));
      } else if (ChecklistbankClient.CANCELED.equalsIgnoreCase(importStatus)) {
        status = Status.ABORTED;
      }
    } catch (Exception e) {
      LOG.warn("Error getting the ChecklistBank import of validation {}", validation.getKey(), e);
    }
    service.updateChecklistValidatorStatus(validation, status);
  }
}
