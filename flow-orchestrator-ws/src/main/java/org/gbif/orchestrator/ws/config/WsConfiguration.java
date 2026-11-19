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
package org.gbif.orchestrator.ws.config;

import org.gbif.orchestrator.ws.file.DownloadFileManager;
import org.gbif.orchestrator.ws.file.FileStoreManager;

import java.io.IOException;
import java.time.Clock;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;

/** Beans of the validation web service that are not components themselves. */
@Configuration
@EnableAsync
public class WsConfiguration {

  /** Name of the multipart part holding the uploaded file. */
  public static final String FILE_POST_PARAM_NAME = "file";

  @Bean
  public Clock clock() {
    return Clock.systemUTC();
  }

  /**
   * The shared validation store ({@code storePath}): each validation's files live in {@code
   * storePath/<key>/}, where the pipelines workers read them.
   */
  @Bean
  public FileStoreManager fileStoreManager(
      @Value("${spring.servlet.multipart.location}") String workingDirectory,
      @Value("${storePath}") String storePath,
      DownloadFileManager downloadFileManager)
      throws IOException {
    return new FileStoreManager(workingDirectory, storePath, downloadFileManager);
  }
}
