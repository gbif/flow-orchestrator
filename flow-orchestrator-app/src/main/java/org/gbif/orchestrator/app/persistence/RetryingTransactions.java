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
package org.gbif.orchestrator.app.persistence;

import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Runs work in a transaction and re-runs it from a fresh read when another transaction changed the
 * same workflow first (optimistic conflict) or inserted the same key (duplicate key). Gives up
 * after {@link #MAX_TRIES} and rethrows, so the message is dead-lettered.
 */
@Component
public class RetryingTransactions {

  public static final int MAX_TRIES = 3;

  private static final Logger LOG = LoggerFactory.getLogger(RetryingTransactions.class);

  private final TransactionTemplate transactions;

  public RetryingTransactions(TransactionTemplate transactions) {
    this.transactions = transactions;
  }

  /**
   * @param what describes the work in logs, for example {@code "commandId=…"}
   * @param work the transactional work; must not return null
   */
  public <T> T execute(String what, Supplier<T> work) {
    for (int attempt = 1; ; attempt++) {
      try {
        return transactions.execute(status -> work.get());
      } catch (OptimisticConflictException | DuplicateKeyException e) {
        if (attempt >= MAX_TRIES) {
          throw e;
        }
        LOG.info(
            "Concurrent change while handling {}, retrying ({}/{}): {}",
            what,
            attempt,
            MAX_TRIES,
            e.getMessage());
      }
    }
  }
}
