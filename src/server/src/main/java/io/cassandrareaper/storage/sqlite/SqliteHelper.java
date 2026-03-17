/*
 * Copyright 2025-2025 DataStax, Inc.
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

import java.time.LocalDate;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper utilities for SQLite storage operations, primarily for JSON serialization/deserialization.
 */
public final class SqliteHelper {

  private static final Logger LOG = LoggerFactory.getLogger(SqliteHelper.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private SqliteHelper() {
    throw new UnsupportedOperationException("Utility class");
  }

  /**
   * Convert an object to JSON string for storage.
   *
   * @param obj The object to convert
   * @return JSON string representation, or null if obj is null
   */
  public static String toJson(Object obj) {
    if (obj == null) {
      return null;
    }
    try {
      return OBJECT_MAPPER.writeValueAsString(obj);
    } catch (JsonProcessingException e) {
      LOG.warn("Failed to serialize object to JSON", e);
      return null;
    }
  }

  /**
   * Convert a JSON string back to an object.
   *
   * @param json The JSON string
   * @param clazz The target class
   * @param <T> The type parameter
   * @return The deserialized object, or null if json is null or deserialization fails
   */
  public static <T> T fromJson(String json, Class<T> clazz) {
    if (json == null || json.isEmpty()) {
      return null;
    }
    try {
      return OBJECT_MAPPER.readValue(json, clazz);
    } catch (JsonProcessingException e) {
      LOG.warn("Failed to deserialize JSON to {}: {}", clazz.getSimpleName(), json, e);
      return null;
    }
  }

  /**
   * Convert a JSON string back to an object using TypeReference.
   *
   * @param json The JSON string
   * @param typeRef The target type reference
   * @param <T> The type parameter
   * @return The deserialized object, or null if json is null or deserialization fails
   */
  public static <T> T fromJson(String json, TypeReference<T> typeRef) {
    if (json == null || json.isEmpty()) {
      return null;
    }
    try {
      return OBJECT_MAPPER.readValue(json, typeRef);
    } catch (JsonProcessingException e) {
      LOG.warn("Failed to deserialize JSON: {}", json, e);
      return null;
    }
  }

  /**
   * Get the ObjectMapper instance for advanced usage.
   *
   * @return The ObjectMapper
   */
  public static ObjectMapper getObjectMapper() {
    return OBJECT_MAPPER;
  }

  /**
   * Convert DateTime to epoch milliseconds for SQLite storage.
   *
   * @param dateTime The DateTime to convert
   * @return Epoch milliseconds, or null if dateTime is null
   */
  public static Long toEpochMilli(DateTime dateTime) {
    return dateTime != null ? dateTime.getMillis() : null;
  }

  /**
   * Convert epoch milliseconds back to DateTime.
   *
   * @param epochMilli The epoch milliseconds
   * @return DateTime object, or null if epochMilli is null
   */
  public static DateTime fromEpochMilli(Long epochMilli) {
    return epochMilli != null && epochMilli > 0 ? new DateTime(epochMilli) : null;
  }

  /**
   * Deserialize a JSON string to a Map<String, String>.
   *
   * @param json The JSON string
   * @return The map, or null if json is null or deserialization fails
   */
  public static Map<String, String> fromJsonStringMap(String json) {
    return fromJson(json, new TypeReference<Map<String, String>>() {});
  }

  /**
   * Deserialize a JSON string to a Collection of Strings (Set, List, etc.).
   *
   * @param json The JSON string
   * @param collectionClass The concrete collection class to instantiate (e.g., HashSet.class)
   * @param <C> The collection type
   * @return The collection, or an empty collection if json is null or deserialization fails
   */
  @SuppressWarnings("unchecked")
  public static <C extends Collection<String>> C fromJsonStringCollection(
      String json, Class<C> collectionClass) {
    if (json == null || json.isEmpty()) {
      try {
        return collectionClass.getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        LOG.warn(
            "Failed to create empty collection of type {}", collectionClass.getSimpleName(), e);
        return (C) new HashSet<String>();
      }
    }
    try {
      // Deserialize to Collection<String> to avoid Set-specific type issues
      Collection<String> items =
          OBJECT_MAPPER.readValue(json, new TypeReference<Collection<String>>() {});
      // Create and populate the target collection type
      C collection = collectionClass.getDeclaredConstructor().newInstance();
      collection.addAll(items);
      return collection;
    } catch (Exception e) {
      LOG.warn(
          "Failed to deserialize JSON to collection {}: {}",
          collectionClass.getSimpleName(),
          json,
          e);
      try {
        return collectionClass.getDeclaredConstructor().newInstance();
      } catch (Exception ex) {
        return (C) new HashSet<String>();
      }
    }
  }

  /**
   * Convert LocalDate to epoch milliseconds for SQLite storage.
   *
   * @param localDate The LocalDate to convert
   * @return Epoch milliseconds (days * 86400000), or null if localDate is null
   */
  public static Long toEpochMilliFromLocalDate(LocalDate localDate) {
    return localDate != null ? localDate.toEpochDay() * 86400000L : null;
  }

  /**
   * Convert epoch milliseconds back to LocalDate.
   *
   * @param epochMilli The epoch milliseconds
   * @return LocalDate object, or null if epochMilli is null
   */
  public static LocalDate fromEpochMilliToLocalDate(Long epochMilli) {
    return epochMilli != null && epochMilli > 0
        ? LocalDate.ofEpochDay(epochMilli / 86400000L)
        : null;
  }

  /**
   * Safely convert a ResultSet Object to Long. SQLite returns Integer for small values and Long for
   * large values.
   *
   * @param value Object from ResultSet (can be Integer, Long, or null)
   * @return Long value, or null
   */
  public static Long toLong(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Long) {
      return (Long) value;
    }
    if (value instanceof Integer) {
      return ((Integer) value).longValue();
    }
    throw new IllegalArgumentException("Cannot convert " + value.getClass().getName() + " to Long");
  }
}
