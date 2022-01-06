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

import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.config.Ini;
import org.apache.shiro.io.ResourceUtils;
import org.apache.shiro.web.config.WebIniSecurityManagerFactory;
import org.junit.Test;


public final class LoginResourceTest {

  @Test
  public void testShiroConfig() throws IOException {
    try (InputStream is = ResourceUtils.getInputStreamForPath("classpath:shiro.ini")) {
      Ini ini = new Ini();
      ini.load(is);
      ini.get("main").remove("filterChainResolver.globalFilters");
      new WebIniSecurityManagerFactory(ini).getInstance();
    }
  }

  @Test
  public void testLogin() throws IOException {
    try (InputStream is = ResourceUtils.getInputStreamForPath("classpath:shiro.ini")) {
      Ini ini = new Ini();
      ini.load(is);
      ini.get("main").remove("filterChainResolver.globalFilters");
      new WebIniSecurityManagerFactory(ini).getInstance().authenticate(new UsernamePasswordToken("admin", "admin"));
    }
  }

}
