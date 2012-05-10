package sys.net.impl.tcp;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


public class KryoBufferPool {

    public static final int BUFFER_POOL_SIZE = 32;
    
    final BlockingQueue<KryoBuffer> bufferPool;

    public KryoBufferPool() {
        this(BUFFER_POOL_SIZE);
    }

    public KryoBufferPool( int size ) {
        bufferPool = new ArrayBlockingQueue<KryoBuffer>(size);
    }
    
    public KryoBuffer take() {
         try {
            return bufferPool.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }
    
    public void offer( KryoBuffer buffer ) {
        bufferPool.offer( buffer);
    }
    
    public int size() {
    	return bufferPool.size();
    }
    
    public int remainingCapacity() {
    	return bufferPool.remainingCapacity();
    }
}
