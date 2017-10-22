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

package io.cassandrareaper.resources.view;


import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public final class RepairRunStatusTest {

  @Test
  public void testRoundIntensity() throws Exception {
    assertEquals(0.0f, RepairRunStatus.roundDoubleNicely(0.0f), 0.00000f);
    assertEquals(0.1f, RepairRunStatus.roundDoubleNicely(0.1f), 0.00001f);
    assertEquals(0.2f, RepairRunStatus.roundDoubleNicely(0.2f), 0.00001f);
    assertEquals(0.3f, RepairRunStatus.roundDoubleNicely(0.3f), 0.00001f);
    assertEquals(0.4f, RepairRunStatus.roundDoubleNicely(0.4f), 0.00001f);
    assertEquals(0.5f, RepairRunStatus.roundDoubleNicely(0.5f), 0.00001f);
    assertEquals(0.6f, RepairRunStatus.roundDoubleNicely(0.6f), 0.00001f);
    assertEquals(0.7f, RepairRunStatus.roundDoubleNicely(0.7f), 0.00001f);
    assertEquals(0.8f, RepairRunStatus.roundDoubleNicely(0.8f), 0.00001f);
    assertEquals(0.9f, RepairRunStatus.roundDoubleNicely(0.9f), 0.00001f);
    assertEquals(1.0f, RepairRunStatus.roundDoubleNicely(1.0f), 0.00001f);
  }

  @Test
  public void testDateTimeToISO8601() {
    DateTime dateTime = new DateTime(2015, 2, 20, 15, 24, 45, DateTimeZone.UTC);
    assertEquals("2015-02-20T15:24:45Z", RepairRunStatus.dateTimeToIso8601(dateTime));
  }
}
