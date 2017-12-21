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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Optional;


public final class Result<SuccessT, FailureT> {
  @Nonnull
  private final Optional<SuccessT> success;
  @Nonnull
  private final Optional<FailureT> failure;

  private Result(@Nonnull Optional<SuccessT> success, @Nonnull Optional<FailureT> failure) {
    this.success = success;
    this.failure = failure;
  }

  public boolean isSucceeded() {
    return !failure.isPresent();
  }

  public boolean isFailed() {
    return !isSucceeded();
  }

  @Nonnull
  public SuccessT getSuccess() {
    return success.get();
  }

  @Nonnull
  public FailureT getFailure() {
    return failure.get();
  }

  public static Result success() {
    return new Result<>(Optional.absent(), Optional.absent());
  }

  public static <SuccessT> Result success(@Nullable SuccessT success) {
    return new Result<>(Optional.of(success), Optional.absent());
  }

  public static <FailureT>  Result fail(@Nonnull FailureT failure) {
    return new Result<>(Optional.absent(), Optional.of(failure));
  }
}
