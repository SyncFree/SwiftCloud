package sys.scheduler;

import static sys.utils.Log.Log;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import sys.utils.Threading;

/**
 * A scheduler for scheduling tasks in real time.
 * 
 * @author smd
 * 
 * @param <T>
 */
public class RT_Scheduler<T extends Task> extends VT_Scheduler<T> {
	private static final int MAX_IDLE_IMEOUT = 30;
	private static final int CORE_POOL_THREADS = 32;
	private static final int MAX_POOL_THREADS = 64;

	BlockingQueue<Runnable> holdQueue = new ArrayBlockingQueue<Runnable>(256);
	BlockingQueue<Runnable> holdQueue2 = new LinkedBlockingQueue<Runnable>();
	ThreadPoolExecutor threadPool = new ThreadPoolExecutor(CORE_POOL_THREADS, MAX_POOL_THREADS, MAX_IDLE_IMEOUT, TimeUnit.SECONDS, holdQueue);

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

		threadPool.prestartAllCoreThreads();
		threadPool.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
		Threading.newThread(this, false).start();

		new PeriodicTask(0.0, 5.0) {
			public void run() {
				// Log.finest("Scheduler.executorPool: " +
				// threadPool.getActiveCount() + " / " +
				// threadPool.getPoolSize() + " / " +
				// threadPool.getMaximumPoolSize() + " / " +
				// threadPool.getQueue().size() );
			}
		};

	}

	final double MIN_WAIT = 0.001;

	@Override
	public void run() {

		while (!stopped) {

			synchronized (queue) {
				double w = 1;
				while (queue.isEmpty() || (w = queue.peek().due - rt_now()) > MIN_WAIT)
					Threading.waitOn(queue, 1 + (int) (750 * w));

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
			double w = task.due - rt_now();
			if (w > MIN_WAIT)
				Threading.sleep((int) (1000 * w));

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
