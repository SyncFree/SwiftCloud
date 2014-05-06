package sys.utils;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.SortedSet;
import java.util.TreeSet;

abstract public class Timings {

    static boolean doTimings = false;

    public static void disable(boolean disable) {
        doTimings = !disable;
    }

    abstract void doStart();

    abstract void doMark();

    abstract void doSample(String msg);

    abstract String doReport();

    public static void start() {
        if (doTimings)
            impl.get().doStart();
    }

    public static void mark() {
        if (doTimings)
            impl.get().doMark();
    }

    public static void sample(String msg) {
        if (doTimings)
            impl.get().doSample(msg);
    }

    public static String report() {
        if (doTimings)
            return impl.get().doReport();
        else
            return EMPTY_STRING;
    }

    private static final ThreadLocal<Timings> impl = new ThreadLocal<Timings>() {
        @Override
        protected Timings initialValue() {
            return new DoTimings();
        }
    };

    private static final String EMPTY_STRING = "";
}

class DoTimings extends Timings {

    Deque<Long> stack = new ArrayDeque<Long>();
    SortedSet<Sample> samples = new TreeSet<Sample>();

    @Override
    public void doStart() {
        samples.clear();
        stack.clear();
        doMark();
        doMark();
    }

    @Override
    public void doMark() {
        stack.push(System.nanoTime());
    }

    @Override
    public void doSample(String msg) {
        long start = stack.pop();
        samples.add(new Sample(msg, start, System.nanoTime()));
    }

    @Override
    public String doReport() {
        sample("Total");
        StringBuilder sb = new StringBuilder();
        for (Sample i : samples) {
            sb.append(String.format("%s ( %.1f ms )", i.msg, i.elapsed())).append("  ");
        }
        return sb.toString();
    }

    static class Sample implements Comparable<Sample> {
        String msg;
        long start, finish;

        Sample(String msg, long start, long finish) {
            this.msg = msg;
            this.start = start;
            this.finish = finish;
        }

        double elapsed() {
            return timeUnit(finish - start);
        }

        @Override
        public int compareTo(Sample other) {
            return (int) Math.signum(this.finish - other.finish);
        }
    }

    static private double timeUnit(long val) {
        return val / 1000000.0; // ms
    }
}