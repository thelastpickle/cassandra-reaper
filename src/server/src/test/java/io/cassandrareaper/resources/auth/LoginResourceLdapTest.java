/*
 *
 * Copyright 2019-2019 The Last Pickle Ltd
 *
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

package io.cassandrareaper.resources.auth;

import java.io.IOException;
import java.io.InputStream;

import static io.cassandrareaper.resources.auth.EmbeddedLdapTest.DOMAIN_DSN;

import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.config.Ini;
import org.apache.shiro.io.ResourceUtils;
import org.apache.shiro.web.config.WebIniSecurityManagerFactory;
import org.junit.Rule;
import org.junit.Test;
import org.zapodot.junit.ldap.EmbeddedLdapRule;
import org.zapodot.junit.ldap.EmbeddedLdapRuleBuilder;

public final class LoginResourceLdapTest {

  @Rule
  public EmbeddedLdapRule embeddedLdapRule =
      EmbeddedLdapRuleBuilder.newInstance()
          .usingDomainDsn(DOMAIN_DSN)
          .importingLdifs("test-ldap-users.ldif")
          .build();

  @Test
  public void testLoginLdap() throws IOException {
    try (InputStream is = ResourceUtils.getInputStreamForPath("classpath:test-shiro-ldap.ini")) {
      Ini ini = new Ini();
      ini.load(is);
      ini.get("main").remove("filterChainResolver.globalFilters");
      int port = embeddedLdapRule.embeddedServerPort();
      ini.setSectionProperty("main", "ldapRealm.contextFactory.url", "ldap://localhost:" + port);
      new WebIniSecurityManagerFactory(ini)
          .getInstance()
          .authenticate(new UsernamePasswordToken("sclaus", "abcdefg"));
    }
  }
}
