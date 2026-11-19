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
package org.gbif.orchestrator.ws;

import org.gbif.ws.security.AppKeySigningService;
import org.gbif.ws.security.FileSystemKeyStore;
import org.gbif.ws.security.GbifAuthenticationManagerImpl;
import org.gbif.ws.server.filter.AppIdentityFilter;
import org.gbif.ws.server.filter.IdentityFilter;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * flow-orchestrator-ws: the user-facing validation API (formerly pipelines-validator-ws).
 *
 * <p>Authentication is remote, against the GBIF API, through gbif-common-ws (as validator-ws);
 * its signing and identity filters for registry itself are excluded.
 */
// Feign is used only through gbif-common-ws client builders; its Spring Cloud auto-configuration
// needs spring-cloud-commons, which this service does not use.
@SpringBootApplication(
    excludeName = "org.springframework.cloud.openfeign.FeignAutoConfiguration")
@EnableConfigurationProperties(WsProperties.class)
@EnableScheduling
@ComponentScan(
    basePackages = {
      "org.gbif.orchestrator.ws",
      "org.gbif.ws.server.interceptor",
      "org.gbif.ws.server.aspect",
      "org.gbif.ws.server.filter",
      "org.gbif.ws.server.advice",
      "org.gbif.ws.server.mapper",
      "org.gbif.ws.remoteauth",
      "org.gbif.ws.security"
    },
    excludeFilters =
        @ComponentScan.Filter(
            type = FilterType.ASSIGNABLE_TYPE,
            classes = {
              AppKeySigningService.class,
              FileSystemKeyStore.class,
              IdentityFilter.class,
              AppIdentityFilter.class,
              GbifAuthenticationManagerImpl.class
            }))
public class Main {

  public static void main(String[] args) {
    System.setProperty("org.apache.tomcat.util.buf.UDecoder.ALLOW_ENCODED_SLASH", "true");
    SpringApplication.run(Main.class, args);
  }
}
