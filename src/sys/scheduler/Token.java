package sys.scheduler;

import static sys.scheduler.VT_Scheduler.Scheduler;

import java.util.concurrent.Semaphore;

public class Token extends Semaphore {

	Token() {
		super(0);
	}

	public void block() {
		Scheduler.threadManager.acquire(this);
	}

	public void unblock() {
		Scheduler.threadManager.release(this);
	}

	private static final long serialVersionUID = 1L;
}