package swift.pubsub;

import java.util.HashMap;
import java.util.Map;

import swift.proto.SwiftProtocolHandler;
import sys.utils.FifoQueue;

public class FifoQueues {

    synchronized FifoQueue<SwiftNotification> queueFor(String id, final SwiftProtocolHandler handler) {
        FifoQueue<SwiftNotification> res = fifoQueues.get(id);
        if (res == null)
            fifoQueues.put(id, res = new FifoQueue<SwiftNotification>() {
                public void process(SwiftNotification event) {
                    event.deliverTo(null, handler);
                }
            });
        return res;
    }

    final Map<String, FifoQueue<SwiftNotification>> fifoQueues = new HashMap<String, FifoQueue<SwiftNotification>>();
}
