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
package org.gbif.orchestrator.ws.mail;

import java.nio.charset.StandardCharsets;
import java.util.Locale;

import org.springframework.context.support.ReloadableResourceBundleMessageSource;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public enum ValidatorEmailType implements EmailType {
  SUCCESSFUL("successfulValidation", "successful_validation.ftl"),

  FAILED("failedValidation", "failed_validation.ftl");

  private static final ReloadableResourceBundleMessageSource MESSAGE_SOURCE;

  static {
    MESSAGE_SOURCE = new ReloadableResourceBundleMessageSource();
    MESSAGE_SOURCE.setBasename("classpath:email/subjects/validation_email_subjects");
    MESSAGE_SOURCE.setDefaultEncoding(StandardCharsets.UTF_8.displayName());
  }

  private final String key;
  private final String template;

  @Override
  public String getKey() {
    return key;
  }

  @Override
  public String getTemplate() {
    return template;
  }

  @Override
  public String getSubject(Locale locale, String... subjectParams) {
    return MESSAGE_SOURCE.getMessage(this.getKey(), subjectParams, locale);
  }
}
