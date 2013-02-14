package sys.net.impl.providers.jvm;

import static sys.net.impl.NetworkingConstants.KRYOBUFFERPOOL_CLT_MAXSIZE;
import static sys.net.impl.NetworkingConstants.KRYOBUFFERPOOL_DELAY;
import static sys.net.impl.NetworkingConstants.KRYOBUFFERPOOL_MAXUSES;

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
                Threading.sleep(KRYOBUFFERPOOL_DELAY);
        }
        return res;
    }

    public void offer(KryoBuffer v) {
        if (v == null)
            return;

        if (totBufs.get() < maxSize && v.uses() < KRYOBUFFERPOOL_MAXUSES)
            queue.add(v);
        else {
            totBufs.decrementAndGet();
        }
    }
}
