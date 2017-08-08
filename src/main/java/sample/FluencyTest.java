package sample;

import org.komamitsu.fluency.BufferFullException;
import org.komamitsu.fluency.Fluency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.komamitsu.fluency.Fluency;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FluencyTest {
    private static final Logger LOG = LoggerFactory.getLogger(FluencyTest.class);
    private ExecutorService executor;
    private final Fluency fluentLogger;

    public FluencyTest() throws IOException {
        this.fluentLogger = setupFluentdLogger();
    }

    public Fluency setupFluentdLogger() throws IOException {
        Fluency.Config fConf = new Fluency.Config().setAckResponseMode(true).setMaxBufferSize(Long.valueOf(128 * 1024 * 1024L));
        return Fluency.defaultFluency(fConf);
    }

    public void shutdown() {
        LOG.info("Shutting down fluency");

        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(3000, TimeUnit.MILLISECONDS)) {
                    LOG.error("Timed out waiting for consumer threads to shut down, exiting uncleanly");
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                LOG.error("Interrupted during shutdown, exiting uncleanly");
                executor.shutdownNow();
            }
        }

        try {
            fluentLogger.close();
            for (int i  =  0; i < 30; i++) {
                if (fluentLogger.isTerminated())
                    break;

                try {
                    TimeUnit.SECONDS.sleep(2);
                } catch (InterruptedException e) {}
            }
        } catch (IOException e) {
            LOG.error("failed to close fluentd logger completely", e);
        }
    }

    public void run() {
        int numThreads = 1;

        // now create an object to consume the messages
        executor = Executors.newFixedThreadPool(numThreads);
        executor.submit(new FluentdHandler(fluentLogger, executor));
    }

    public static void main(String[] args) throws IOException {
        final FluencyTest fluency = new FluencyTest();

        fluency.run();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                fluency.shutdown();
            }
        }));

        try {
            // Need better long running approach.
            while (!Thread.currentThread().isInterrupted()) {
                Thread.sleep(10000);
            }
        } catch (InterruptedException e) {
            LOG.error("Something happen!", e);
        }
    }
}
