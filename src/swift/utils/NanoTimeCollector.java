package swift.utils;

/**
 * Measuring time between two events.
 * 
 * @author annettebieniusa
 * 
 */
public class NanoTimeCollector {

    private long time;
    private long duration;
    private long total;
    private int iterations;

    public NanoTimeCollector() {
        duration = 0;
        time = 0;
        iterations = 0;
        total = 0;
    }

    /**
     * . Starts the timer
     */
    public void start() {
        time = System.currentTimeMillis();
    }

    /**
     * Returns in the duration since timer start. Additionally, total duration
     * gets accumulated, and number of iterations increased.
     * 
     * @return duration since last timer start
     */
    public long stop() {
        time = System.currentTimeMillis() - time;
        duration += time;
        total += time;
        iterations++;
        return time;
    }

    /**
     * Resets the total duration.
     */
    public void reset() {
        duration = 0;
        iterations = 0;
        total = 0;
    }

    /**
     * Returns the accumulated duration of all measured timer intervals since
     * the creation of timer or the last timer reset.
     * 
     * @return accumulated duration of all measured timer intervals
     */
    public long getTotalDuration() {
        return total;
    }

    /**
     * Returns the average of all time intervals measured since creating of
     * timer or the last timer reset.
     * 
     * It is often a good idea to reset the timer after several iterations to
     * exclude "bad" intervals due to system setup, just-in-time compilation
     * etc.
     * 
     * @return average duration of all measured timer intervals
     */
    public long getAverage() {
        return total / (long) iterations;
    }
}
