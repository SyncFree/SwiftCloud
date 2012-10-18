package sys.scheduler;

import java.util.LinkedList;
import java.util.logging.Logger;

import sys.utils.Threading;

/**
 * A scheduler manages a priority queue of tasks, issuing them when they are due
 * for execution.
 * 
 * New tasks and re-scheduled tasks are inserted into the priority queue and
 * sorted according to their execution deadlines. In each iteration, the
 * scheduler picks the next task to execute and advances virtual time
 * accordingly. Therefore, virtual time advances in discrete steps and is
 * decoupled from real time. Depending on the number of tasks in the queue and
 * the time spent in their execution, virtual time can run faster or slower than
 * real time.
 * 
 * @author Sergio Duarte (smd@fct.unl.pt)
 * 
 */
public class VT_Scheduler<T extends Task> implements Runnable {
	private static Logger Log = Logger.getLogger( VT_Scheduler.class.getName() );

	protected static final double NANOSECOND = 1e-9;

	static VT_Scheduler<?> Scheduler;
	protected boolean stopped;

	protected VT_Scheduler() {
		Scheduler = this;
		queue = new CustomPriorityQueue<Task>();
	}

	public Token newToken() {
		return new Token();
	}

	/**
	 * Stops the scheduler, preventing further tasks to be executed...
	 */
	public void stop() {
		stopped = true;
	}

	/**
	 * Starts the scheduler and begins executing tasks in order.
	 */
	public void start() {
		new SchedulerThread(Scheduler).start();
		Threading.waitOn(this);
	}

	/**
	 * Returns the number of simulation seconds that elapsed since the
	 * simulation started.
	 * 
	 * @return The number of simulation seconds that elapsed since the
	 *         simulation started.
	 */
	public double now() {
		return now;
	}

	/**
	 * Returns the number of realtime seconds that elapsed since the simulation
	 * started.
	 * 
	 * @return The number of realtime seconds that elapsed since the simulation
	 *         started.
	 */
	public double rt_now() {
		return (System.nanoTime() - rt0) * NANOSECOND;
	}

	/**
	 * Cancels all tasks in the scheduler queue, effectively ending the
	 * simulation.
	 */
	public void cancelAll() {
		for (Task i : queue)
			i.cancel();
	}

	/**
	 * Inserts a new task in the scheduler queue with a given execution
	 * deadline.
	 * 
	 * @param t
	 *            The task to be scheduled.
	 * @param due
	 *            The execution deadline of the task, relative to the current
	 *            simulation time.
	 * @return
	 */
	Task schedule(Task t, double due) {
		assert !t.isCancelled && !t.isQueued;
		t.due = now() + Math.max(NANOSECOND, due);
		queue.add(t);
		return t;
	}

	/**
	 * Re-inserts a task back into the scheduler queue with an updated execution
	 * deadline.
	 * 
	 * @param t
	 *            The task to be re-scheduled.
	 * @param due
	 *            The new execution deadline of the task, relative to the
	 *            current simulation time.
	 */
	void reSchedule(Task t, double due) {
		assert !t.isCancelled;

		if (t.isQueued) {
			double oldValue = t.due;
			t.due = now() + Math.max(NANOSECOND, due);
			queue.changeKey(t, oldValue);
		} else {
			t.due = now() + Math.max(NANOSECOND, due);
			queue.add(t);
		}
	}

	/*
	 * (non-Javadoc) The main loop of the scheduler. In each iteration the next
	 * task is picked for execution. Additionally, from time to time, a call is
	 * made to the Gui to check if there are any pending graphical elements that
	 * need to be rendered. Gui (backbuffer) rendering is thread-safe if it is
	 * done by the (current) thread running in the scheduler.
	 * 
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		try {
			mainLoop();
		} catch (KillThreadException x) {
		}

		if (stopped)
			Threading.notifyOn(this);
	}

	protected void mainLoop() {
		while (!queue.isEmpty() && !stopped) {
			processNextTask();
		}
		stopped = true;
	}

	/**
	 * 
	 * Resumes the execution of one of the ready threads (previously blocked in
	 * a network I/O operation) or picks the next task to execute.
	 * 
	 */
	protected void processNextTask() {

		threadManager.relieve();
		Task next = queue.peek();

		SchedulerThread.setTask(next);
		if (next == null || next.isCancelled) {
			queue.poll();
			return;
		}

		now = next.due;
		rt1 = System.nanoTime();
		try {
			next.reset();
			next.run();
			next.reSchedule();
			if (next.isQueued && (next.isCancelled || !next.wasReScheduled))
				queue.remove();

		} catch (Exception x) {
			queue.remove();
			Log.severe("Offending task cancelled...");
			x.printStackTrace();
		}
	}

	public boolean isStopped() {
		return stopped;
	}

	protected static double now = 0;
	protected final CustomPriorityQueue<Task> queue;
	protected double rt1 = System.nanoTime(), rt0 = System.nanoTime();
	protected final ThreadManager threadManager = new ThreadManager();

	/**
	 * This class manages the threads used in the simulation. The invariant
	 * observed is that only one scheduler thread is executing at a given time.
	 * Therefore, there is no need for synchronization of data structures.
	 * 
	 * The simulation uses additional threads for the GUI, but all the drawing
	 * is actually done to a back buffer image by the scheduler thread currently
	 * running.
	 * 
	 * This manager is required to implement blocking network operations. When a
	 * blocking io operation is performed within a task, the current (scheduler)
	 * thread is blocked until the following read is ready, in the mean time,
	 * the scheduler picks a previously blocked thread to continue execution or
	 * picks other tasks for execution.
	 * 
	 * @author Sérgio Duarte (smd@di.fct.unl.pt)
	 * 
	 */
	protected class ThreadManager {

		private LinkedList<Token> spareThreads = new LinkedList<Token>();
		private LinkedList<Token> readyThreads = new LinkedList<Token>();
		private LinkedList<Token> waitingThreads = new LinkedList<Token>();

		void relieve() {
			if (readyThreads.size() > 0) {
				if (spareThreads.size() < 2) {
					Token t = new Token();
					spareThreads.addLast(t);
					readyThreads.removeFirst().release();
					t.acquireUninterruptibly();
				} else {
					readyThreads.removeFirst().release();
					throw new KillThreadException();
				}
			}
		}

		void release(Token token) {
			readyThreads.addLast(token);
			waitingThreads.remove(token);
		}

		void acquire(Token token) {
			queue.remove(SchedulerThread.getTask());

			waitingThreads.add(token);
			if (readyThreads.size() > 0)
				readyThreads.removeFirst().release();
			else if (spareThreads.size() > 0)
				spareThreads.removeFirst().release();
			else {
				new SchedulerThread(Scheduler).start();
			}
			token.acquireUninterruptibly();
		}

		@Override
		public String toString() {
			int total = 1 + readyThreads.size() + waitingThreads.size() + spareThreads.size();
			return String.format("Threads: (T:%d)<R:%d/W:%d/S:%d>\n", total, readyThreads.size(), waitingThreads.size(), spareThreads.size());
		}
	}

}

class SchedulerThread extends Thread {
	Task task;

	SchedulerThread(Runnable r) {
		super(r);
		super.setDaemon(true);
	}

	static void setTask(Task t) {
		SchedulerThread st = (SchedulerThread) currentThread();
		st.task = t;
	}

	static Task getTask() {
		SchedulerThread st = (SchedulerThread) currentThread();
		return st.task;
	}
}

class KillThreadException extends RuntimeException {
	private static final long serialVersionUID = 1L;
}
