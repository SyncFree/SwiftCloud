package swift.pubsub;

import java.util.concurrent.ConcurrentHashMap;

import swift.proto.SwiftProtocolHandler;
import sys.utils.FifoQueue;

public class FifoQueues {

    public FifoQueue<SwiftNotification> queueFor(String id, final SwiftProtocolHandler handler) {
        FifoQueue<SwiftNotification> res = fifoQueues.get(id), nq;
        if (res == null) {
            res = fifoQueues.putIfAbsent(id, nq = new FifoQueue<SwiftNotification>() {
                public void process(SwiftNotification event) {
                    event.deliverTo(null, handler);
                }
            });
            if (res == null)
                res = nq;
        }
        return res;
    }

    final ConcurrentHashMap<String, FifoQueue<SwiftNotification>> fifoQueues = new ConcurrentHashMap<String, FifoQueue<SwiftNotification>>();
}
