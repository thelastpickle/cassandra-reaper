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

package io.cassandrareaper.storage.postgresql;

import org.postgresql.util.PSQLException;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;

public final class JdbiExceptionUtil {

  private static final String DUPLICATE_KEY_CODE = "23505";

  private JdbiExceptionUtil() {
  }

  public static boolean isDuplicateKeyError(UnableToExecuteStatementException exc) {
    if (exc.getCause() instanceof PSQLException) {
      return ((PSQLException) exc.getCause()).getSQLState().equals(DUPLICATE_KEY_CODE);
    }
    return false;
  }
}
