package sys.net.impl.providers;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static sys.net.impl.NetworkingConstants.*;

public class BufferPool<V> {

	final BlockingQueue<V> bufferPool;

	public BufferPool() {
		this(KRYOBUFFERPOOL_SIZE);
	}

	public BufferPool(int size) {
		bufferPool = new ArrayBlockingQueue<V>(size);
	}

	public V poll() {
		try {
			return bufferPool.poll( KRYOBUFFERPOOL_TIMEOUT, TimeUnit.MILLISECONDS ) ;
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}

	public V take() {
		try {
			return bufferPool.take();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public void offer(V buffer) {
		bufferPool.offer(buffer);
	}

	public int size() {
		return bufferPool.size();
	}

	public int remainingCapacity() {
		return bufferPool.remainingCapacity();
	}
}
