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
package org.gbif.orchestrator.ws.support;

import org.gbif.orchestrator.ws.api.Validation;
import org.gbif.orchestrator.ws.mail.ValidatorEmailService;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** Records the validations an email would be sent for (as stored when sending). */
public class RecordingEmails implements ValidatorEmailService {

  public final List<Validation> sent = new CopyOnWriteArrayList<>();
  public volatile boolean fail;

  @Override
  public void sendEmailNotification(Validation validation) {
    if (fail) {
      throw new IllegalStateException("SMTP down");
    }
    sent.add(validation);
  }
}
