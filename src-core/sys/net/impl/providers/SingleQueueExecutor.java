package sys.net.impl.providers;

import static sys.net.impl.NetworkingConstants.*;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class SingleQueueExecutor {
	final BlockingQueue<Runnable> holdQueue = new ArrayBlockingQueue<Runnable>(NIO_EXEC_QUEUE_SIZE);
	final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(NIO_CORE_POOL_THREADS, NIO_MAX_POOL_THREADS, NIO_MAX_IDLE_THREAD_IMEOUT, TimeUnit.SECONDS, holdQueue);

	public SingleQueueExecutor() {
		threadPool.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
	}

	public void execute(final Object queueSelector, final Runnable task) {
		threadPool.execute(task);
	}
}
