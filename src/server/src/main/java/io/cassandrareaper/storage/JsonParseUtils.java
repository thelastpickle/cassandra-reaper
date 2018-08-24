/*
 * Copyright 2018-2018 The Last Pickle Ltd
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

import io.cassandrareaper.ReaperException;
import io.cassandrareaper.service.RingRange;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JsonParseUtils {

  private static final Logger LOG = LoggerFactory.getLogger(JsonParseUtils.class);

  private JsonParseUtils() {
    throw new IllegalStateException("Utility class");
  }

  private static <T> T parseJson(String json, TypeReference<T> ref) {
    try {
      return new ObjectMapper().readValue(json, ref);
    } catch (IOException e) {
      LOG.error("error parsing json", e);
      throw new RuntimeException(e);
    }
  }

  public static List<RingRange> parseRingRangeList(Optional<String> json) {
    if (json.isPresent()) {
      return parseJson(json.get(), new TypeReference<List<RingRange>>() {});
    }

    return Lists.newArrayList();
  }

  public static String writeTokenRangesTxt(List<RingRange> tokenRanges) throws ReaperException {
    try {
      return new ObjectMapper().writeValueAsString(tokenRanges);
    } catch (JsonProcessingException e) {
      throw new ReaperException(e);
    }
  }
}
