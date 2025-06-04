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

package io.cassandrareaper.resources;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.HttpMethod;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class RequestUtilsTest {
  @Test
  public void testIsOptionsRequestInvalidInputReturnsFalse() {
    boolean isOptionsRequest = RequestUtils.isOptionsRequest(null);
    Assertions.assertThat(isOptionsRequest).isFalse();
  }

  @Test
  public void testIsOptionsRequestOptionsServletInputReturnsTrue() {
    HttpServletRequest mockServletRequest = spy(HttpServletRequest.class);
    when(mockServletRequest.getMethod()).thenReturn(HttpMethod.OPTIONS);
    boolean isOptionsRequest = RequestUtils.isOptionsRequest(mockServletRequest);
    Assertions.assertThat(isOptionsRequest).isTrue();
  }

  @Test
  public void testIsOptionsRequestGetServletInputReturnsTrue() {
    HttpServletRequest mockServletRequest = spy(HttpServletRequest.class);
    when(mockServletRequest.getMethod()).thenReturn(HttpMethod.GET);
    boolean isOptionsRequest = RequestUtils.isOptionsRequest(mockServletRequest);
    Assertions.assertThat(isOptionsRequest).isFalse();
  }
}
