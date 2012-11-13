package sys.net.impl.providers;

import static sys.net.impl.NetworkingConstants.*;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import sys.scheduler.PeriodicTask;

// TODO: Instead of blocking on take(), might be better to use poll with adaptative timeout,
// based on exponential backoff. This would allow accepting new tasks, that would execute out of order but
// still tend to enforce some load balancing...
// To ensure fairness, the timeout probably needs to be on a per queue basis...

public class MultiQueueExecutor {
	final static int QUEUE_ARRAY_SIZE = 101;

	final Semaphore queues[];
	final BlockingQueue<Runnable> holdQueue = new LinkedBlockingQueue<Runnable>();
	final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(NIO_CORE_POOL_THREADS, NIO_MAX_POOL_THREADS, NIO_MAX_IDLE_THREAD_IMEOUT, TimeUnit.SECONDS, holdQueue);

	public MultiQueueExecutor() {
		queues = new Semaphore[QUEUE_ARRAY_SIZE];
		for (int i = 0; i < queues.length; i++)
			queues[i] = new Semaphore(NIO_EXEC_QUEUE_SIZE, true);
			
		threadPool.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
		
//		new PeriodicTask(0.0, 5.0) {
//			public void run() {
//				System.err.println(sys.Sys.Sys.mainClass + "->" + holdQueue.size() );
//			}
//		};
	}

	public void execute(final Object queueSelector, final Runnable task) {
		Semaphore queue = queues[( queueSelector.hashCode() >>> 1) % QUEUE_ARRAY_SIZE];
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
