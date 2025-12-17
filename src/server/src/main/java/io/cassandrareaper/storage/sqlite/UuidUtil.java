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

import java.nio.ByteBuffer;
import java.util.UUID;

/** Utility class for converting UUIDs to/from byte arrays for SQLite BLOB storage. */
public final class UuidUtil {

  private UuidUtil() {
    throw new UnsupportedOperationException("Utility class");
  }

  /**
   * Convert a UUID to a byte array for SQLite BLOB storage.
   *
   * @param uuid The UUID to convert
   * @return A 16-byte array representing the UUID
   */
  public static byte[] toBytes(UUID uuid) {
    if (uuid == null) {
      return null;
    }
    ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    return bb.array();
  }

  /**
   * Convert a byte array from SQLite BLOB storage back to a UUID.
   *
   * @param bytes The byte array to convert (must be 16 bytes)
   * @return The reconstructed UUID
   */
  public static UUID fromBytes(byte[] bytes) {
    if (bytes == null || bytes.length != 16) {
      return null;
    }
    ByteBuffer bb = ByteBuffer.wrap(bytes);
    long mostSigBits = bb.getLong();
    long leastSigBits = bb.getLong();
    return new UUID(mostSigBits, leastSigBits);
  }
}
