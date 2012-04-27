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

    public NanoTimeCollector() {
        duration = 0;
        time = 0;
    }

    /**
     * . Starts the timer
     */
    public void start() {
        time = System.nanoTime();
    }

    /**
     * Returns in the duration since timer start. Additionally, duration gets
     * accumulated.
     * 
     * @return duration since last timer start
     */
    public long stop() {
        time = System.nanoTime() - time;
        duration += time;
        return duration;
    }

    /**
     * Resets the total duration.
     */
    public void reset() {
        duration = 0;
    }

    /**
     * Returns the accumulated duration of all measured timer intervals since
     * the creation of timer or the last timer reset.
     * 
     * @return accumulated duration of all measured timer intervals
     */
    public long getTotalDuration() {
        return duration;
    }

}
