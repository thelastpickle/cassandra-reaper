/*
 * Copyright 2024 The Last Pickle Ltd
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

package io.cassandrareaper.storage.sqlite;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import org.joda.time.DateTime;
import org.junit.Test;

public final class SqliteHelperTest {

  @Test
  public void testToLongFromInteger() {
    Integer intValue = 42;
    Long result = SqliteHelper.toLong(intValue);
    assertNotNull(result);
    assertEquals(Long.valueOf(42L), result);
  }

  @Test
  public void testToLongFromLong() {
    Long longValue = 123456789L;
    Long result = SqliteHelper.toLong(longValue);
    assertNotNull(result);
    assertEquals(longValue, result);
  }

  @Test
  public void testToLongFromNull() {
    Long result = SqliteHelper.toLong(null);
    assertNull(result);
  }

  @Test
  public void testToLongFromZero() {
    Integer zero = 0;
    Long result = SqliteHelper.toLong(zero);
    assertNotNull(result);
    assertEquals(Long.valueOf(0L), result);
  }

  @Test
  public void testToLongFromNegativeInteger() {
    Integer negValue = -100;
    Long result = SqliteHelper.toLong(negValue);
    assertNotNull(result);
    assertEquals(Long.valueOf(-100L), result);
  }

  @Test
  public void testToLongFromMaxInteger() {
    Integer maxInt = Integer.MAX_VALUE;
    Long result = SqliteHelper.toLong(maxInt);
    assertNotNull(result);
    assertEquals(Long.valueOf(Integer.MAX_VALUE), result);
  }

  @Test
  public void testToLongFromMinInteger() {
    Integer minInt = Integer.MIN_VALUE;
    Long result = SqliteHelper.toLong(minInt);
    assertNotNull(result);
    assertEquals(Long.valueOf(Integer.MIN_VALUE), result);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testToLongFromInvalidType() {
    SqliteHelper.toLong("not a number");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testToLongFromDouble() {
    SqliteHelper.toLong(42.5);
  }

  @Test
  public void testToEpochMilliFromDateTime() {
    DateTime now = DateTime.now();
    Long epochMilli = SqliteHelper.toEpochMilli(now);
    assertNotNull(epochMilli);
    assertEquals(now.getMillis(), epochMilli.longValue());
  }

  @Test
  public void testToEpochMilliFromNull() {
    Long result = SqliteHelper.toEpochMilli(null);
    assertNull(result);
  }

  @Test
  public void testFromEpochMilliToDateTime() {
    long timestamp = 1234567890123L;
    DateTime result = SqliteHelper.fromEpochMilli(timestamp);
    assertNotNull(result);
    assertEquals(timestamp, result.getMillis());
  }

  @Test
  public void testFromEpochMilliFromNull() {
    DateTime result = SqliteHelper.fromEpochMilli(null);
    assertNull(result);
  }

  @Test
  public void testRoundTripDateTimeConversion() {
    DateTime original = DateTime.now();
    Long epochMilli = SqliteHelper.toEpochMilli(original);
    DateTime restored = SqliteHelper.fromEpochMilli(epochMilli);

    assertNotNull(restored);
    assertEquals(original.getMillis(), restored.getMillis());
  }

  @Test
  public void testToJsonFromSet() {
    Set<String> testSet = new HashSet<>(Arrays.asList("item1", "item2", "item3"));
    String json = SqliteHelper.toJson(testSet);

    assertNotNull(json);
    assertTrue(json.contains("item1"));
    assertTrue(json.contains("item2"));
    assertTrue(json.contains("item3"));
  }

  @Test
  public void testToJsonFromEmptySet() {
    Set<String> emptySet = Collections.emptySet();
    String json = SqliteHelper.toJson(emptySet);

    assertNotNull(json);
    assertEquals("[]", json);
  }

  @Test
  public void testToJsonFromNull() {
    String json = SqliteHelper.toJson(null);
    assertNull(json);
  }

  @Test
  public void testFromJsonToSet() {
    String json = "[\"item1\",\"item2\",\"item3\"]";
    Set<String> result = SqliteHelper.fromJson(json, new TypeReference<Set<String>>() {});

    assertNotNull(result);
    assertEquals(3, result.size());
    assertTrue(result.contains("item1"));
    assertTrue(result.contains("item2"));
    assertTrue(result.contains("item3"));
  }

  @Test
  public void testFromJsonEmptyArray() {
    String json = "[]";
    Set<String> result = SqliteHelper.fromJson(json, new TypeReference<Set<String>>() {});

    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testFromJsonNull() {
    Set<String> result = SqliteHelper.fromJson(null, new TypeReference<Set<String>>() {});
    assertNull(result);
  }

  @Test
  public void testFromJsonStringCollectionToList() {
    String json = "[\"a\",\"b\",\"c\"]";
    List<String> result = SqliteHelper.fromJsonStringCollection(json, ArrayList.class);

    assertNotNull(result);
    assertEquals(3, result.size());
    assertTrue(result.contains("a"));
    assertTrue(result.contains("b"));
    assertTrue(result.contains("c"));
  }

  @Test
  public void testFromJsonStringCollectionToSet() {
    String json = "[\"x\",\"y\",\"z\"]";
    Set<String> result = SqliteHelper.fromJsonStringCollection(json, HashSet.class);

    assertNotNull(result);
    assertEquals(3, result.size());
    assertTrue(result.contains("x"));
    assertTrue(result.contains("y"));
    assertTrue(result.contains("z"));
  }

  @Test
  public void testFromJsonStringCollectionEmpty() {
    String json = "[]";
    Set<String> result = SqliteHelper.fromJsonStringCollection(json, HashSet.class);

    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testFromJsonStringCollectionNull() {
    Set<String> result = SqliteHelper.fromJsonStringCollection(null, HashSet.class);
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testRoundTripJsonConversion() {
    Set<String> original = new HashSet<>(Arrays.asList("test1", "test2", "test3"));
    String json = SqliteHelper.toJson(original);
    Set<String> restored = SqliteHelper.fromJson(json, new TypeReference<Set<String>>() {});

    assertNotNull(restored);
    assertEquals(original.size(), restored.size());
    assertTrue(restored.containsAll(original));
  }

  @Test
  public void testToJsonWithSpecialCharacters() {
    Set<String> testSet =
        new HashSet<>(Arrays.asList("item\"with\"quotes", "item\nwith\nnewlines"));
    String json = SqliteHelper.toJson(testSet);

    assertNotNull(json);
    // Verify JSON is properly escaped
    assertTrue(json.contains("\\\"") || json.contains("\\n"));
  }

  @Test
  public void testFromJsonWithUnicodeCharacters() {
    String json = "[\"Hello 世界\",\"Привет мир\",\"مرحبا بالعالم\"]";
    Set<String> result = SqliteHelper.fromJson(json, new TypeReference<Set<String>>() {});

    assertNotNull(result);
    assertEquals(3, result.size());
  }
}
