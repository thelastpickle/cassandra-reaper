/*
 * Copyright 2018-2019 The Last Pickle Ltd
 *
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

package io.cassandrareaper.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.Lists;
import io.cassandrareaper.service.RingRange;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JsonParseUtils {

  private static final Logger LOG = LoggerFactory.getLogger(JsonParseUtils.class);

  private static final ObjectReader LIST_READER =
      new ObjectMapper().readerFor(new TypeReference<List<RingRange>>() {});
  private static final ObjectReader MAP_READER =
      new ObjectMapper().readerFor(new TypeReference<Map<String, String>>() {});

  private static final ObjectWriter WRITER = new ObjectMapper().writer();

  private JsonParseUtils() {
    throw new IllegalStateException("Utility class");
  }

  public static List<RingRange> parseRingRangeList(Optional<String> json) {
    try {
      return json.isPresent() ? LIST_READER.readValue(json.get()) : Lists.newArrayList();
    } catch (IOException e) {
      LOG.error("error parsing json", e);
      throw new IllegalArgumentException(e);
    }
  }

  public static String writeTokenRangesTxt(List<RingRange> tokenRanges) {
    try {
      return WRITER.writeValueAsString(tokenRanges);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public static Map<String, String> parseReplicas(Optional<String> json) {
    try {
      return json.isPresent() ? MAP_READER.readValue(json.get()) : Collections.emptyMap();
    } catch (IOException e) {
      LOG.error("error parsing json", e);
      throw new IllegalArgumentException(e);
    }
  }

  public static String writeReplicas(Map<String, String> replicas) {
    try {
      return WRITER.writeValueAsString(replicas);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
