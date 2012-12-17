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
package sys.scheduler;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import sys.utils.Threading;

/**
 * A scheduler for scheduling tasks in real time.
 * 
 * @author smd
 * 
 * @param <T>
 */
public class RT_Scheduler<T extends Task> extends VT_Scheduler<T> {

    private static Logger Log = Logger.getLogger(RT_Scheduler.class.getName());

    ExecutorService threadPool = Executors.newCachedThreadPool();

    protected RT_Scheduler() {
        super();
    }

    @Override
    Task schedule(Task t, double due) {
        assert !t.isCancelled && !t.isQueued;
        t.due = now() + Math.max(NANOSECOND, due);
        synchronized (queue) {
            queue.add(t);
            t.isQueued = true;
            Threading.notifyAllOn(queue);
        }
        return t;
    }

    @Override
    void reSchedule(Task t, double due) {
        assert !t.isCancelled;

        synchronized (queue) {
            if (t.isQueued)
                queue.remove(t);
            t.due = now() + Math.max(NANOSECOND, due);

            queue.add(t);
            t.isQueued = true;
            Threading.notifyOn(queue);
        }
    }

    @Override
    public double now() {
        return super.rt_now();
    }

    @Override
    public void start() {
        Log.fine("Task scheduler starting...");
        Threading.newThread("scheduler-dispatcher", this, false).start();
    }

    final double MIN_WAIT = 0.005;

    @Override
    public void run() {

        while (!stopped) {

            synchronized (queue) {
                double w = 1.0;
                while (queue.isEmpty() || (w = queue.peek().due - rt_now()) > MIN_WAIT)
                    Threading.waitOn(queue, (int) (1000 * w));

                threadPool.execute(new Runnable() {
                    Task t = queue.remove();

                    public void run() {
                        executeTask(t);
                    }
                });
            }
        }
        Log.fine("Task scheduler stopping...");
    }

    public void executeTask(Task task) {
        if (task != null && !task.isCancelled) {
            try {
                task.reset();
                task.run();
                task.reSchedule();
            } catch (Exception x) {
                Log.severe("Offending task cancelled...");
                x.printStackTrace();
            }
        }

    }
}
