package sys.scheduler;

import static sys.utils.Log.Log;

import java.util.concurrent.atomic.AtomicInteger;

import sys.utils.Threading;

/**
 * A scheduler for scheduling tasks in real time.
 * 
 * @author smd
 * 
 * @param <T>
 */
public class RT_Scheduler<T extends Task> extends VT_Scheduler<T> {
	private static final int MAX_IDLE_THREADS = 2;

	AtomicInteger idleThreads = new AtomicInteger(0);

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
			Threading.notifyOn(queue);
		}
		return t;
	}

	@Override
	public double now() {
		return super.rt_now();
	}

	@Override
	public void start() {
		Log.fine("Task scheduler starting...");

		Threading.newThread(new Runnable() {
			// thread pool watchdog...
			@Override
			public void run() {
				do {
					if (idleThreads.intValue() < MAX_IDLE_THREADS) {
						idleThreads.incrementAndGet();
						Threading.newThread(RT_Scheduler.this, false).start();
					}
					Threading.sleep(50);
				} while (!stopped);
			}
		}, false).start();
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
	public void run() {

		final double MIN_WAIT = 1;
		while (!stopped) {
			Task task;

			synchronized (queue) {

				while (queue.isEmpty())
					Threading.waitOn(queue);

				task = queue.peek();
				double w = 1000 * (task.due - rt_now());
				if (w > MIN_WAIT) {
					Threading.waitOn(queue, (int) w);
					continue;
				}
				idleThreads.decrementAndGet();

				task = queue.remove();
			}

			if (!task.isCancelled) {
				try {
					task.reset();
					task.run();
					task.reSchedule();
				} catch (Exception x) {
					Log.severe("Offending task cancelled...");
					x.printStackTrace();
				}
			}

			if (idleThreads.intValue() <= MAX_IDLE_THREADS)
				idleThreads.incrementAndGet();
			else
				return;

		}
		Log.fine("Task scheduler stopping...");
	}
}
