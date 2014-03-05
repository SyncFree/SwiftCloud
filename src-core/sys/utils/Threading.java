package sys.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A collection of convenience methods for dealing with threads.
 * 
 * @author smduarte (smd@fct.unl.pt)
 * 
 */
public class Threading {

    protected Threading() {
    }

    static public Thread newThread(boolean daemon, Runnable r) {
        Thread res = new Thread(r);
        res.setDaemon(daemon);
        return res;
    }

    static public Thread newThread(Runnable r, boolean daemon) {
        Thread res = new Thread(r);
        res.setDaemon(daemon);
        return res;
    }

    static public Thread newThread(String name, boolean daemon, Runnable r) {
        Thread res = new Thread(r);
        res.setName(Thread.currentThread() + "." + name);
        res.setDaemon(daemon);
        return res;
    }

    static public Thread newThread(String name, Runnable r, boolean daemon) {
        Thread res = new Thread(r);
        res.setName(Thread.currentThread() + "." + name);
        res.setDaemon(daemon);
        return res;
    }

    static public void sleep(long ms) {
        try {
            if (ms > 0)
                Thread.sleep(ms);
        } catch (InterruptedException x) {
            x.printStackTrace();
        }
    }

    static public void sleep(long ms, int ns) {
        try {
            if (ms > 0 || ns > 0)
                Thread.sleep(ms, ns);
        } catch (InterruptedException x) {
            x.printStackTrace();
        }
    }

    static public void waitOn(Object o) {
        try {
            o.wait();
        } catch (InterruptedException x) {
            x.printStackTrace();
        }
    }

    static public void waitOn(Object o, long ms) {
        try {
            if (ms > 0)
                o.wait(ms);
        } catch (InterruptedException x) {
            x.printStackTrace();
        }
    }

    static public void notifyOn(Object o) {
        o.notify();
    }

    static public void notifyAllOn(Object o) {
        o.notifyAll();
    }

    static public void synchronizedWaitOn(Object o) {
        synchronized (o) {
            try {
                o.wait();
            } catch (InterruptedException x) {
                x.printStackTrace();
            }
        }
    }

    static public void synchronizedWaitOn(Object o, long ms) {
        synchronized (o) {
            try {
                o.wait(ms);
            } catch (InterruptedException x) {
                x.printStackTrace();
            }
        }
    }

    static public void synchronizedNotifyOn(Object o) {
        synchronized (o) {
            o.notify();
        }
    }

    static public void synchronizedNotifyAllOn(Object o) {
        synchronized (o) {
            o.notifyAll();
        }
    }

    public static class LockManager {

        /**
         * Returns an object "value" (rather than any instance) to serve as a
         * monitor. Leaks memory...
         * 
         * @param o
         * @return
         */
        synchronized public Object lockFor(Object o) {

            Object res = locks.get(o);
            if (res == null) {
                res = o;
                locks.put(res, counter.incrementAndGet());
            }
            return res;
        }

        synchronized public void freeLockFor(Object o) {
            locks.remove(o);
        }

        AtomicInteger counter = new AtomicInteger(0);
        Map<Object, Object> locks = new HashMap<Object, Object>();
    }

    synchronized public static void dumpAllThreadsTraces() {
        Map<Thread, StackTraceElement[]> traces = Thread.getAllStackTraces();
        loop: for (StackTraceElement[] trace : traces.values()) {
            for (int j = 0; j < trace.length; j++)
                if (trace[j].getClassName().startsWith("swift")) {
                    for (int k = j; k < trace.length; k++)
                        System.err.print(">>" + trace[k] + " ");
                    System.err.println();
                    continue loop;
                }
        }

    }

    static public ThreadFactory factory(final String name) {
        return new ThreadFactory() {
            int counter = 0;
            String callerName = Thread.currentThread().getName();

            @Override
            public Thread newThread(Runnable target) {
                Thread t = new Thread(target, callerName + "." + name + "-" + counter++);
                t.setDaemon(true);
                return t;
            }
        };
    }
}
