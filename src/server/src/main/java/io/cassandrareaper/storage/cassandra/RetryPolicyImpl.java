package io.cassandrareaper.storage.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
import jakarta.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Retry policy implementation for Reaper. */
public final class RetryPolicyImpl implements RetryPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(RetryPolicyImpl.class);
  private static final int MAX_READ_RETRIES = 10;
  private static final int MAX_WRITE_RETRIES = 10;
  private static final int BASE_DELAY_MS = 100;
  private static final int MAX_DELAY_MS = 10000;

  private final String logPrefix;

  public RetryPolicyImpl(DriverContext context, String profileName) {
    this(context.getSessionName() + "|" + profileName);
  }

  public RetryPolicyImpl(String logPrefix) {
    this.logPrefix = logPrefix;
  }

  private int calculateBackoffDelay(int retryCount) {
    int delay = BASE_DELAY_MS * (1 << retryCount);
    return Math.min(delay, MAX_DELAY_MS);
  }

  private boolean isIdempotent(Request request) {
    return Boolean.TRUE.equals(request.isIdempotent());
  }

  private RetryDecision retryIfIdempotent(
      Request request, int retry, int maxRetries, String methodName) {
    if (request == null) {
      LOG.warn("{} Received null request in {}, rethrowing", logPrefix, methodName);
      return RetryDecision.RETHROW;
    }
    if (retry >= maxRetries) {
      LOG.warn(
          "{} Max retries ({}) exceeded in {} for request {}",
          logPrefix,
          maxRetries,
          methodName,
          request);
      return RetryDecision.RETHROW;
    }
    if (isIdempotent(request)) {
      try {
        Thread.sleep(calculateBackoffDelay(retry));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return RetryDecision.RETHROW;
      }
      return RetryDecision.RETRY_NEXT;
    }
    return RetryDecision.RETHROW;
  }

  @Override
  public RetryDecision onReadTimeout(
      @NotNull Request request,
      @NotNull ConsistencyLevel cl,
      int required,
      int received,
      boolean retrieved,
      int retry) {
    return retryIfIdempotent(request, retry, MAX_READ_RETRIES, "onReadTimeout");
  }

  @Override
  public RetryDecision onWriteTimeout(
      @NotNull Request request,
      @NotNull ConsistencyLevel cl,
      @NotNull WriteType type,
      int required,
      int received,
      int retry) {
    return retryIfIdempotent(request, retry, MAX_WRITE_RETRIES, "onWriteTimeout");
  }

  @Override
  public RetryDecision onUnavailable(
      @NotNull Request request,
      @NotNull ConsistencyLevel consistencyLevel,
      int required,
      int received,
      int retry) {
    return retryIfIdempotent(request, retry, MAX_READ_RETRIES, "onUnavailable");
  }

  @Override
  public RetryDecision onRequestAborted(
      @NotNull Request request, @NotNull Throwable throwable, int retry) {
    return retryIfIdempotent(request, retry, MAX_READ_RETRIES, "onRequestAborted");
  }

  @Override
  public RetryDecision onErrorResponse(
      @NotNull Request request, @NotNull CoordinatorException exception, int retry) {
    return retryIfIdempotent(request, retry, MAX_READ_RETRIES, "onErrorResponse");
  }

  @Override
  public void close() {}
}
