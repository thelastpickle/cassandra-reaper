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

package com.spotify.reaper.resources.view;


import com.spotify.reaper.resources.CommonTools;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public final class RepairRunStatusTest {

  @Test
  public void testRoundIntensity() throws Exception {
    assertEquals(0.0f, CommonTools.roundDoubleNicely(0.0f), 0.00000f);
    assertEquals(0.1f, CommonTools.roundDoubleNicely(0.1f), 0.00001f);
    assertEquals(0.2f, CommonTools.roundDoubleNicely(0.2f), 0.00001f);
    assertEquals(0.3f, CommonTools.roundDoubleNicely(0.3f), 0.00001f);
    assertEquals(0.4f, CommonTools.roundDoubleNicely(0.4f), 0.00001f);
    assertEquals(0.5f, CommonTools.roundDoubleNicely(0.5f), 0.00001f);
    assertEquals(0.6f, CommonTools.roundDoubleNicely(0.6f), 0.00001f);
    assertEquals(0.7f, CommonTools.roundDoubleNicely(0.7f), 0.00001f);
    assertEquals(0.8f, CommonTools.roundDoubleNicely(0.8f), 0.00001f);
    assertEquals(0.9f, CommonTools.roundDoubleNicely(0.9f), 0.00001f);
    assertEquals(1.0f, CommonTools.roundDoubleNicely(1.0f), 0.00001f);
  }
}
