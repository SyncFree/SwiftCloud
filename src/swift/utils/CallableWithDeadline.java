package swift.utils;

import java.util.concurrent.Callable;

/**
 * A task with deadline that can be retried a number of times in case of
 * (transient) failure.
 * 
 * @author mzawirski
 */
public abstract class CallableWithDeadline<V> implements Callable<V> {
    private final long deadlineTime;

    /**
     * Creates callable task without deadline.
     */
    public CallableWithDeadline() {
        this.deadlineTime = Long.MAX_VALUE;
    }

    /**
     * Creates callable task with provided deadline in milliseconds.
     */
    public CallableWithDeadline(long deadlineMillis) {
        this.deadlineTime = System.currentTimeMillis() + deadlineMillis;
    }

    /**
     * Executes (or retries) a task. Throws an exception if task needs to be
     * retried.
     * 
     * @see Callable#call()
     */
    @Override
    public V call() throws Exception {
        final V result = callOrFailWithNull();
        if (result == null) {
            throw new Exception("Task needs a retry");
        }
        return result;
    }

    /**
     * Executes a task.
     * 
     * @return return value, or null if task should be retried.
     */
    protected abstract V callOrFailWithNull();

    public int getDeadlineLeft() {
        // This is (int) only for RPC lib convenience.
        return (int) Math.min(Math.max(deadlineTime - System.currentTimeMillis(), 0), Integer.MAX_VALUE);
    }

    public long getDeadlineTime() {
        return deadlineTime;
    }
}
