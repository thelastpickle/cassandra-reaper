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

package com.spotify.reaper.storage.cassandra;


import java.nio.ByteBuffer;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import org.joda.time.DateTime;

public final class DateTimeCodec extends TypeCodec<DateTime> {

  public DateTimeCodec() {
    super(DataType.timestamp(), DateTime.class);
  }

  @Override
  public DateTime parse(String value) {
    if (value == null || value.equals("NULL")) {
      return null;
    }

    try {
      return DateTime.parse(value);
    } catch (IllegalArgumentException iae) {
      throw new InvalidTypeException("Could not parse format: " + value, iae);
    }
  }

  @Override
  public String format(DateTime value) {
    if (value == null) {
      return "NULL";
    }

    return Long.toString(value.getMillis());
  }

  @Override
  public ByteBuffer serialize(DateTime value, ProtocolVersion protocolVersion) {
    return value == null ? null : BigintCodec.INSTANCE.serializeNoBoxing(value.getMillis(), protocolVersion);
  }

  @Override
  public DateTime deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
    return bytes == null || bytes.remaining() == 0
        ? null
        : new DateTime(BigintCodec.INSTANCE.deserializeNoBoxing(bytes, protocolVersion));
  }

  /**
   * Base class for codecs handling CQL 8-byte integer types such as {@link DataType#bigint()}, {@link
   * DataType#counter()} or {@link DataType#time()}.
   */
  private abstract static class LongCodec extends PrimitiveLongCodec {

    private LongCodec(DataType cqlType) {
      super(cqlType);
    }

    @Override
    public Long parse(String value) {
      try {
        return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL") ? null : Long.parseLong(value);
      } catch (NumberFormatException e) {
        throw new InvalidTypeException(String.format("Cannot parse 64-bits long value from \"%s\"", value));
      }
    }

    @Override
    public String format(Long value) {
      if (value == null) {
        return "NULL";
      }
      return Long.toString(value);
    }

    @Override
    public ByteBuffer serializeNoBoxing(long value, ProtocolVersion protocolVersion) {
      ByteBuffer bb = ByteBuffer.allocate(8);
      bb.putLong(0, value);
      return bb;
    }

    @Override
    public long deserializeNoBoxing(ByteBuffer bytes, ProtocolVersion protocolVersion) {
      if (bytes == null || bytes.remaining() == 0) {
        return 0;
      }
      if (bytes.remaining() != 8) {
        throw new InvalidTypeException("Invalid 64-bits long value, expecting 8 bytes but got " + bytes.remaining());
      }

      return bytes.getLong(bytes.position());
    }
  }

  private static final class BigintCodec extends LongCodec {

    private static final BigintCodec INSTANCE = new BigintCodec();

    private BigintCodec() {
      super(DataType.bigint());
    }
  }
}
