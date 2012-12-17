/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package sys.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A collection of convenience methods for dealing with threads.
 * 
 * @author Sérgio Duarte (smd@fct.unl.pt)
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
        res.setName(name);
        res.setDaemon(daemon);
        return res;
    }

    static public Thread newThread(String name, Runnable r, boolean daemon) {
        Thread res = new Thread(r);
        res.setName(name);
        res.setDaemon(daemon);
        return res;
    }

    static public void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException x) {
            x.printStackTrace();
        }
    }

    static public void sleep(long ms, int ns) {
        try {
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

    static public void waitOn(Object o, int ms) {
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
}
