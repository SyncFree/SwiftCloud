package sys.pubsub.impl;

import java.util.concurrent.atomic.AtomicLong;

import sys.pubsub.PubSub;
import sys.pubsub.PubSub.Notifyable;

public abstract class AbstractSubscriber<T> implements PubSub.Subscriber<T> {

    final String id;
    final private AtomicLong fifoSeq = new AtomicLong(0L);

    public AbstractSubscriber(String id) {
        this.id = id;
    }

    public int hashCode() {
        return id.hashCode();
    }

    public boolean equals(Object other) {
        return other instanceof AbstractSubscriber && id.equals(((AbstractSubscriber<?>) other).id);
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public long nextSeqN() {
        return fifoSeq.incrementAndGet();
    }

    @Override
    public void onNotification(Notifyable<T> info) {
        Thread.dumpStack();
    }

}
