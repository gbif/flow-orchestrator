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

import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.QueueBinding;
import org.gbif.common.messaging.api.MessageCallback;
import org.gbif.orchestrator.app.config.OrchestratorProperties;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Starts consuming commands and completions once the application context is ready, and stops on
 * shutdown. Declares each queue with its dead-letter exchange and {@code <queue>.dlq}.
 */
public class ListenerLifecycle implements SmartLifecycle {

  private static final Logger LOG = LoggerFactory.getLogger(ListenerLifecycle.class);

  private final MessageListener listener;
  private final OrchestratorProperties.Messaging settings;
  private final CommandListener commands;
  private final CompletionListener completions;
  private final DwcDpAnalysisListener analysis;
  private volatile boolean running;

  public ListenerLifecycle(
      MessageListener listener,
      OrchestratorProperties.Messaging settings,
      CommandListener commands,
      CompletionListener completions,
      DwcDpAnalysisListener analysis) {
    this.listener = listener;
    this.settings = settings;
    this.commands = commands;
    this.completions = completions;
    this.analysis = analysis;
  }

  @Override
  public synchronized void start() {
    if (running) {
      return;
    }
    listen(settings.getCommands(), commands);
    listen(settings.getCompletions(), completions);
    if (!settings.getDwcdpAnalysisFinished().isUnset()) {
      listen(settings.getDwcdpAnalysisFinished(), analysis);
    }
    running = true;
  }

  private <T> void listen(OrchestratorProperties.Listener queue, MessageCallback<T> callback) {
    QueueBinding binding =
        QueueBinding.builder(queue.getQueue(), queue.getExchange(), queue.getRoutingKey())
            .threads(queue.getThreads())
            .deadLetter(settings.getDeadLetterExchange())
            .build();
    try {
      listener.listen(binding, callback);
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot listen on " + binding, e);
    }
    LOG.info("Listening {} for {}", binding, callback.getMessageClass().getSimpleName());
  }

  @Override
  public synchronized void stop() {
    if (running) {
      listener.close();
      running = false;
      LOG.info("Listeners stopped");
    }
  }

  @Override
  public boolean isRunning() {
    return running;
  }
}
