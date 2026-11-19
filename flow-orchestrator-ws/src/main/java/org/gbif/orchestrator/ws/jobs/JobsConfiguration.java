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

import org.gbif.orchestrator.ws.clb.ChecklistbankClient;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.provider.jdbctemplate.JdbcTemplateLockProvider;
import net.javacrumbs.shedlock.spring.annotation.EnableSchedulerLock;

/**
 * Scheduled jobs run on one instance at a time: ShedLock keeps its locks in the {@code shedlock}
 * table of the orchestrator schema (the connection's current schema), using the database's clock.
 */
@Configuration
@EnableSchedulerLock(defaultLockAtMostFor = "PT1H")
public class JobsConfiguration {

  @Bean
  public LockProvider lockProvider(DataSource dataSource) {
    return new JdbcTemplateLockProvider(
        JdbcTemplateLockProvider.Configuration.builder()
            .withJdbcTemplate(new JdbcTemplate(dataSource))
            .withTableName("shedlock")
            .usingDbTime()
            .build());
  }

  @Bean
  public ChecklistbankClient checklistbankClient(
      @Value("${clb.api.url}") String url,
      @Value("${clb.api.user}") String user,
      @Value("${clb.api.password}") String password) {
    return ChecklistbankClient.create(url, user, password);
  }
}
