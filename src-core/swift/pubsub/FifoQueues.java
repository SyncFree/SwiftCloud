package swift.pubsub;

import java.util.concurrent.ConcurrentHashMap;

import swift.crdt.core.CRDTIdentifier;
import sys.pubsub.PubSub;
import sys.pubsub.PubSubNotification;
import sys.utils.FifoQueue;

public class FifoQueues {

    public FifoQueue<PubSubNotification<CRDTIdentifier>> queueFor(Object id,
            final PubSub.Subscriber<CRDTIdentifier> subscriber) {
        FifoQueue<PubSubNotification<CRDTIdentifier>> res = fifoQueues.get(id), nq;
        if (res == null) {
            res = fifoQueues.putIfAbsent(id, nq = new FifoQueue<PubSubNotification<CRDTIdentifier>>() {
                public void process(PubSubNotification<CRDTIdentifier> event) {
                    event.notifyTo(subscriber);
                }
            });
            if (res == null)
                res = nq;
        }
        return res;
    }

    final ConcurrentHashMap<Object, FifoQueue<PubSubNotification<CRDTIdentifier>>> fifoQueues = new ConcurrentHashMap<Object, FifoQueue<PubSubNotification<CRDTIdentifier>>>();
}
