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
package org.gbif.orchestrator.app.messaging;

import org.gbif.common.messaging.api.MessageCallback;
import org.gbif.orchestrator.app.completion.DwcDpAnalysisAdapter;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Consumes the dwc-dp-analyser-service's legacy {@code DwcDpValidationFinished}. A message that
 * cannot be used throws, so postal rejects it and it is dead-lettered.
 */
public class DwcDpAnalysisListener implements MessageCallback<JsonNode> {

  private final DwcDpAnalysisAdapter adapter;

  public DwcDpAnalysisListener(DwcDpAnalysisAdapter adapter) {
    this.adapter = adapter;
  }

  @Override
  public void handleMessage(JsonNode message) {
    adapter.handle(message);
  }

  @Override
  public Class<JsonNode> getMessageClass() {
    return JsonNode.class;
  }
}
