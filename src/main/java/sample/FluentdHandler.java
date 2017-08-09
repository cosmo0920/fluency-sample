package sample;

import org.komamitsu.fluency.BufferFullException;
import org.komamitsu.fluency.Fluency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class FluentdHandler implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(FluentdHandler.class);

    private final Fluency logger;
    private final ExecutorService executor;
    private boolean interruptable;

    public FluentdHandler(Fluency logger, ExecutorService executor, boolean interruptible) {
        this.logger = logger;
        this.executor = executor;
        this.interruptable = interruptible;
    }

    public void run() {
        while (!executor.isShutdown()) {
            if (interruptable) {
                runInternal();
            } else {
                while (true) {
                    runInternal();
                }
            }
        }
    }

    private void runInternal() {
        Exception ex = null;
        
        try {
            emitTestEvent();
        } catch (BufferFullException bfe) {
            LOG.error("fluentd logger reached buffer full. Wait 1 second for retry", bfe);

            while (true) {
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException ie) {
                    LOG.warn("Interrupted during sleep");
                    Thread.currentThread().interrupt();
                }

                try {
                    emitTestEvent();
                    LOG.info("Retry emit succeeded. Buffer full is resolved");
                    break;
                } catch (IOException e) {}

                LOG.error("fluentd logger is still buffer full. Wait 1 second for next retry");
            }
        } catch (IOException e) {
            ex = e;
        }

        if (ex != null) {
            LOG.error("can't send logs to fluentd. Wait 1 second", ex);
            ex = null;
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException ie) {
                LOG.warn("Interrupted during sleep");
                Thread.currentThread().interrupt();
            }
        }
    }

    private void emitTestEvent() throws IOException {
        String tag = "test";
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("name", "fluentd");
        event.put("organization", "fluent");
        event.put("version", 0.14);
        logger.emit(tag, event);
    }
}
