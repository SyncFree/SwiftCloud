package swift.utils;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Executor of {@link CallableWithDeadline} tasks using exponential back-off in
 * case of failure.
 * 
 * @author mzawirski
 */
public class ExponentialBackoffTaskExecutor {
    public static final int FREQ_RETRY_LOGGING = 100;
    private static Logger logger = Logger.getLogger(ExponentialBackoffTaskExecutor.class.getName());

    private final String name;
    private final int initialRetryWaitTimeMillis;
    private final int retryWaitTimeMultiplier;
    private final AtomicInteger retriesNumber;

    public ExponentialBackoffTaskExecutor(final String name, final int initialRetryWaitTimeMillis,
            final int retryWaitTimeMultiplier) {
        this.name = name;
        this.initialRetryWaitTimeMillis = initialRetryWaitTimeMillis;
        this.retryWaitTimeMultiplier = retryWaitTimeMultiplier;
        this.retriesNumber = new AtomicInteger();
    }

    /**
     * Executes retryable task using exponential back-off in case of failure.
     * 
     * @param task
     *            task to execute, throws exception in case of failure that
     *            requires retry
     * @return task result, or null in case of exceeded deadline
     */
    public <V> V execute(final CallableWithDeadline<V> task) {
        int interRetryWaitTime = initialRetryWaitTimeMillis;
        long deadlineLeft = task.getDeadlineLeft();
        while (deadlineLeft >= 0) {
            try {
                return task.call();
            } catch (Exception x) {
                reportRetry();
                interRetryWaitTime *= retryWaitTimeMultiplier;
                deadlineLeft = task.getDeadlineLeft();
                if (interRetryWaitTime <= deadlineLeft) {
                    try {
                        Thread.sleep(Math.min(deadlineLeft, interRetryWaitTime));
                    } catch (InterruptedException e) {
                        // TODO: support interrupts better.
                    }
                } else {
                    deadlineLeft = -1;
                }
            }
        }
        return null;
    }

    private void reportRetry() {
        if (retriesNumber.incrementAndGet() % FREQ_RETRY_LOGGING == 0) {
            logger.warning("Retried " + name + " " + FREQ_RETRY_LOGGING + " times since last log entry.");
        }
    }
}
