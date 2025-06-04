package io.cassandrareaper.storage.cassandra;

import javax.validation.constraints.NotNull;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
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

  @Override
  public RetryDecision onReadTimeout(
      @NotNull Request request,
      @NotNull ConsistencyLevel cl,
      int required,
      int received,
      boolean retrieved,
      int retry) {

    if (retry >= MAX_READ_RETRIES) {
      LOG.warn(
          "{} Max read retries ({}) exceeded for request {}", logPrefix, MAX_READ_RETRIES, request);
      return RetryDecision.RETHROW;
    }

    if (request.isIdempotent()) {
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
  public RetryDecision onWriteTimeout(
      @NotNull Request request,
      @NotNull ConsistencyLevel cl,
      @NotNull WriteType type,
      int required,
      int received,
      int retry) {

    if (retry >= MAX_WRITE_RETRIES) {
      LOG.warn(
          "{} Max write retries ({}) exceeded for request {}",
          logPrefix,
          MAX_WRITE_RETRIES,
          request);
      return RetryDecision.RETHROW;
    }

    if (request.isIdempotent()) {
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
  public RetryDecision onUnavailable(
      @NotNull Request request,
      @NotNull ConsistencyLevel consistencyLevel,
      int required,
      int received,
      int retry) {

    if (retry >= MAX_READ_RETRIES) {
      LOG.warn(
          "{} Max unavailable retries ({}) exceeded for request {}",
          logPrefix,
          MAX_READ_RETRIES,
          request);
      return RetryDecision.RETHROW;
    }

    if (request.isIdempotent()) {
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
  public RetryDecision onRequestAborted(
      @NotNull Request request, @NotNull Throwable throwable, int retry) {

    if (retry >= MAX_READ_RETRIES) {
      LOG.warn(
          "{} Max request abort retries ({}) exceeded for request {}",
          logPrefix,
          MAX_READ_RETRIES,
          request);
      return RetryDecision.RETHROW;
    }

    if (request.isIdempotent()) {
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
  public RetryDecision onErrorResponse(
      @NotNull Request request, @NotNull CoordinatorException exception, int retry) {

    if (retry >= MAX_READ_RETRIES) {
      LOG.warn(
          "{} Max error response retries ({}) exceeded for request {}",
          logPrefix,
          MAX_READ_RETRIES,
          request);
      return RetryDecision.RETHROW;
    }

    if (request.isIdempotent()) {
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
  public void close() {}
}
