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
package sys.net.impl.providers;

import static sys.net.impl.NetworkingConstants.NIO_CORE_POOL_THREADS;
import static sys.net.impl.NetworkingConstants.NIO_EXEC_QUEUE_SIZE;
import static sys.net.impl.NetworkingConstants.NIO_MAX_IDLE_THREAD_IMEOUT;
import static sys.net.impl.NetworkingConstants.NIO_MAX_POOL_THREADS;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

// TODO: Instead of blocking on take(), might be better to use poll with adaptative timeout,
// based on exponential backoff. This would allow accepting new tasks, that would execute out of order but
// still tend to enforce some load balancing...
// To ensure fairness, the timeout probably needs to be on a per queue basis...

public class MultiQueueExecutor {
    final static int QUEUE_ARRAY_SIZE = 101;

    final Semaphore queues[];
    final BlockingQueue<Runnable> holdQueue = new LinkedBlockingQueue<Runnable>();
    final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(NIO_CORE_POOL_THREADS, NIO_MAX_POOL_THREADS,
            NIO_MAX_IDLE_THREAD_IMEOUT, TimeUnit.SECONDS, holdQueue);

    public MultiQueueExecutor() {
        queues = new Semaphore[QUEUE_ARRAY_SIZE];
        for (int i = 0; i < queues.length; i++)
            queues[i] = new Semaphore(NIO_EXEC_QUEUE_SIZE, true);

        threadPool.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

        // new PeriodicTask(0.0, 5.0) {
        // public void run() {
        // System.err.println(sys.Sys.Sys.mainClass + "->" + holdQueue.size() );
        // }
        // };
    }

    public void execute(final Object queueSelector, final Runnable task) {
        Semaphore queue = queues[(queueSelector.hashCode() >>> 1) % QUEUE_ARRAY_SIZE];
        _Task t = new _Task(task, queue);
        queue.acquireUninterruptibly();
        threadPool.execute(t);
    }

    class _Task implements Runnable {

        final Runnable task;
        final Semaphore queue;

        _Task(Runnable task, Semaphore queue) {
            this.queue = queue;
            this.task = task;
        }

        public void run() {
            try {
                task.run();
            } catch (Throwable t) {
                t.printStackTrace();
            }
            queue.release();
        }
    }

}
