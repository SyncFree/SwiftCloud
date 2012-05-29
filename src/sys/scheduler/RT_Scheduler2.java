package sys.scheduler;

import static sys.utils.Log.Log;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import sys.utils.Threading;

/**
 * A scheduler for scheduling tasks in real time.
 * 
 * @author smd
 * 
 * @param <T>
 */
public class RT_Scheduler2<T extends Task> extends VT_Scheduler<T> {
	private static final int MIN_IDLE_THREADS = 2;
	private static final int CORE_POOL_THREADS = 8;
	private static final int MAX_POOL_THREADS = 128;

	AtomicInteger idleThreads = new AtomicInteger(0);
	AtomicInteger poolThreads = new AtomicInteger(0);

	protected RT_Scheduler2() {
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

	ExecutorService pool = Executors.newFixedThreadPool(32);

	@Override
	public void start() {
		Log.fine("Task scheduler starting...");

		Threading.newThread(new Runnable() {
			@Override
			public void run() {
				do {
					while (poolThreads.get() < CORE_POOL_THREADS || (idleThreads.get() < MIN_IDLE_THREADS && poolThreads.get() < MAX_POOL_THREADS)) {
						poolThreads.incrementAndGet();
						idleThreads.incrementAndGet();
						Threading.newThread(RT_Scheduler2.this, false).start();
					}
					Threading.sleep(15);
				} while (!stopped);
			}
		}, false).start();

		new PeriodicTask(0.0, 5.0) {
			public void run() {
				System.err.println(poolThreads.get());
			}
		};

		// // Threading.newThread(new Runnable() {
		// // thread pool watchdog...
		// @Override
		// public void run() {
		// do {
		// if (idleThreads.intValue() < MAX_IDLE_THREADS) {
		// idleThreads.incrementAndGet();
		// Threading.newThread(RT_Scheduler.this, false).start();
		// }
		// Threading.sleep(5);
		// } while (!stopped);
		// }
		// }, false).start();
	}

	@Override
	public void run() {

		final double MIN_WAIT = 0.001;
		while (!stopped) {
			Task task;

			synchronized (queue) {
				while (queue.isEmpty())
					Threading.waitOn(queue);

				idleThreads.decrementAndGet();
				task = queue.remove();
			}

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
			if (idleThreads.intValue() <= MIN_IDLE_THREADS)
				idleThreads.incrementAndGet();
			else {
				if (poolThreads.get() >= CORE_POOL_THREADS) {
					poolThreads.decrementAndGet();
					return;
				}
			}

		}
		Log.fine("Task scheduler stopping...");
	}
}
