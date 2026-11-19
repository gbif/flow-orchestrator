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

import org.gbif.api.model.common.GbifUser;
import org.gbif.api.vocabulary.UserRole;
import org.gbif.ws.security.GbifUserPrincipal;

import java.util.Set;

import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

/** Sets the current user, as the remote authentication filter would. */
public final class Users {

  private Users() {}

  public static Authentication authentication(String username, UserRole... roles) {
    GbifUser user = new GbifUser();
    user.setUserName(username);
    user.setEmail(username + "@example.org");
    user.setRoles(Set.of(roles));
    GbifUserPrincipal principal = new GbifUserPrincipal(user);
    return new UsernamePasswordAuthenticationToken(principal, null, principal.getAuthorities());
  }

  public static void login(String username, UserRole... roles) {
    SecurityContextHolder.getContext().setAuthentication(authentication(username, roles));
  }

  public static void logout() {
    SecurityContextHolder.clearContext();
  }
}
