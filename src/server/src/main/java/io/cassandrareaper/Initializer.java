package io.cassandrareaper;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class Initializer {

    private static class NamedTask {
        String name;
        InitializationTask task;

        public NamedTask(String name, InitializationTask task) {
            this.name = name;
            this.task = task;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(Initializer.class);

    private final ExecutorService tasks;

    private boolean failed;

    private final AppContext context;

    public Initializer(AppContext context) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("initializer").build();
        tasks = Executors.newSingleThreadExecutor(threadFactory);
        this.context = context;
    }

    public void submit(String name, InitializationTask task) throws ReaperException, InterruptedException {
        if (context.config.isInKubernetesSidecarMode()) {
            runInBackground(name, task);
        } else {
            task.execute();
        }
    }

    private void runInBackground(String name, InitializationTask task) {
        tasks.submit(() -> {
            try {
                if (failed) {
                    LOG.info("skipping initialization task {} due to previous failure", name);
                } else {
                    LOG.info("executing initialization task {}", name);
                    task.execute();
                }
            } catch (InterruptedException e) {
                LOG.debug("initialization task " + name + " was interrupted", e);
            } catch (Exception e) {
                LOG.warn("initialization task " + name + " failed", e);
                failed = true;
                context.isRunning.set(false);
            }
        });
    }

}
