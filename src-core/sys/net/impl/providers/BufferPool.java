package sys.net.impl.providers;

import static sys.net.impl.NetworkingConstants.KRYOBUFFERPOOL_CLT_MAXSIZE;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import sys.utils.Threading;

final public class BufferPool {

    int maxSize;
    AtomicInteger totBufs = new AtomicInteger(0);

    static class Buffer {

    }

    static public interface BufferFactory<V> {
        V newBuffer();
    }

    ConcurrentLinkedQueue<KryoBuffer> queue;

    public BufferPool() {
        this(KRYOBUFFERPOOL_CLT_MAXSIZE);
    }

    public BufferPool(int maxSize) {
        this.queue = new ConcurrentLinkedQueue<KryoBuffer>();
        this.maxSize = maxSize;
    }

    public KryoBuffer poll() {
        KryoBuffer res = queue.poll();
        if (res == null) {
            int v = totBufs.incrementAndGet();
            res = new KryoBuffer();
            if (v > maxSize)
                Threading.sleep(100);
        }
        return res;
    }

    public void offer(KryoBuffer v) {
        if (v == null)
            return;

        if (totBufs.get() < maxSize && v.uses() < 4096)
            queue.add(v);
        else {
            totBufs.decrementAndGet();
        }
    }
}
