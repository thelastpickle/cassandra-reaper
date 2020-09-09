package io.cassandrareaper;

public interface InitializationTask {

    void execute() throws ReaperException, InterruptedException;

}
