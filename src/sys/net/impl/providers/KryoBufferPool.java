package sys.net.impl.providers;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class KryoBufferPool {
	private static final int TIMEOUT = 100;
	public static final int BUFFER_POOL_SIZE = 4;

	final BlockingQueue<KryoBuffer> bufferPool;

	public KryoBufferPool() {
		this(BUFFER_POOL_SIZE);
	}

	public KryoBufferPool(int size) {
		bufferPool = new ArrayBlockingQueue<KryoBuffer>(size);
	}

	public KryoBuffer take() {
		try {
			return bufferPool.poll( TIMEOUT, TimeUnit.MILLISECONDS ) ;
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}

	public void offer(KryoBuffer buffer) {
		bufferPool.offer(buffer);
	}

	public int size() {
		return bufferPool.size();
	}

	public int remainingCapacity() {
		return bufferPool.remainingCapacity();
	}
}
