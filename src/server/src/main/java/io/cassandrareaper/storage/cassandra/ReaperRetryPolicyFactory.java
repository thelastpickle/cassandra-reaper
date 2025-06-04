package io.cassandrareaper.storage.cassandra;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import io.dropwizard.cassandra.DropwizardProgrammaticDriverConfigLoaderBuilder;
import io.dropwizard.cassandra.retry.RetryPolicyFactory;

/**
 * Retry all statements.
 *
 * <p>All reaper statements are idempotent. Reaper generates few read and writes requests, so it's
 * ok to keep retrying.
 *
 * <p>Sleep 100 milliseconds in between subsequent read retries. Fail after the tenth read retry.
 *
 * <p>Writes keep retrying forever.
 */
public final class ReaperRetryPolicyFactory implements RetryPolicyFactory {

  @Override
  public void accept(DropwizardProgrammaticDriverConfigLoaderBuilder builder) {
    builder.withClass(DefaultDriverOption.RETRY_POLICY_CLASS, RetryPolicyImpl.class);
  }
}
