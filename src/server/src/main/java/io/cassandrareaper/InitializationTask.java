package io.cassandrareaper;

/**
 * Executes startup related tasks which may be run in a background thread after Reaper has
 * started. Any tasks that can block startup, e.g., connecting to Cassandra, should be run
 * as an InitializationTask.
 */
public interface InitializationTask {

    void execute() throws ReaperException, InterruptedException;

}
