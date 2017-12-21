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

package io.cassandrareaper.core;

public final class Intensity {
  private final double value;

  private Intensity(double value) {
    this.value = value;
  }

  public double get() {
    return value;
  }

  public static Result<Intensity, String> tryCreate(String value) {
    final double intensity;
    try {
      intensity = Double.parseDouble(value);
    } catch (NumberFormatException ex) {
      return Result.fail("invalid intensity value: " + value);
    }

    return tryCreate(intensity);
  }

  public static Result<Intensity, String> tryCreate(double value) {
    if (value <= 0.0 || value > 1.0) {
      return Result.fail("intensity must be in half closed range (0.0, 1.0]: " + value);
    }

    return Result.success(new Intensity(value));
  }
}
