package swift.utils;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import sys.scheduler.PeriodicTask;
import sys.utils.Threading;

/**
 * Executor of {@link CallableWithDeadline} tasks using exponential back-off in
 * case of failure.
 * 
 * @author mzawirski
 */
public class ExponentialBackoffTaskExecutor {
    public static final int LOGGING_RETRY_SAMPLING_FREQ = 100;
    public static final int LOGGING_RETRY_WAIT_THRESHOLD_MS = 60 * 1000;
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
    	totalOps.incrementAndGet();
    	
        int interRetryWaitTime = initialRetryWaitTimeMillis;
        long deadlineLeft = task.getDeadlineLeft();        
        while (deadlineLeft >= 0) {
            try {
            	totalTries.incrementAndGet();
                return task.call();
            } catch (Exception x) {
                interRetryWaitTime *= retryWaitTimeMultiplier;
                
                //smd DEBUG
                interRetryWaitTime = Math.min( 5000, interRetryWaitTime);
                
                reportRetry(interRetryWaitTime, task);
                deadlineLeft = task.getDeadlineLeft();
                if (interRetryWaitTime <= deadlineLeft) {
                	Threading.synchronizedWaitOn( task, (int)Math.min(deadlineLeft, interRetryWaitTime) );                    
                } else {
                    deadlineLeft = -1;
                }
            }
        }
        return null;
    }

    private void reportRetry(final long retryWaitTimeMs) {
        if (retriesNumber.incrementAndGet() % LOGGING_RETRY_SAMPLING_FREQ == 0) {
            logger.warning("Retried " + name + " " + LOGGING_RETRY_SAMPLING_FREQ + " times since last log entry.");
        } else if (retryWaitTimeMs >= LOGGING_RETRY_WAIT_THRESHOLD_MS) {
            logger.warning("Retried " + name + " and waiting back-off already exceeded " + retryWaitTimeMs + "ms.");
        }
    }
    
    //smd for debugging purposes...
    private void reportRetry(final long retryWaitTimeMs, Object task) {
        if (retriesNumber.incrementAndGet() % LOGGING_RETRY_SAMPLING_FREQ == 0) {
            logger.warning("Retried " + name + " " + LOGGING_RETRY_SAMPLING_FREQ + " times since last log entry.");
        } else if (retryWaitTimeMs >= LOGGING_RETRY_WAIT_THRESHOLD_MS) {
            logger.warning("Retried " + name + " and waiting back-off already exceeded " + retryWaitTimeMs + "ms. " + task);
        }
    }
    
    static AtomicInteger totalOps = new AtomicInteger(0);
    static AtomicInteger totalTries = new AtomicInteger(0);
    static {
    	new PeriodicTask(0, 30) {
    		public void run() {
                double mean = totalTries.get() * 1.0 / totalOps.get();
    			if( mean > 1)
    				logger.warning(String.format("ExponentialBackoff Retries: %.1f\n", mean));
    		}
    	};
    }
}
