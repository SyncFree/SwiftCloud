package swift.pubsub;

import static sys.net.api.Networking.Networking;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import swift.crdt.core.CRDTIdentifier;
import swift.dc.DCConstants;
import swift.proto.MetadataStatsCollector;
import swift.proto.PubSubHandshake;
import swift.proto.SwiftProtocolHandler;
import swift.proto.UnsubscribeUpdatesReply;
import swift.proto.UnsubscribeUpdatesRequest;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandle;
import sys.pubsub.impl.AbstractPubSub;
import sys.scheduler.Task;
import sys.utils.FifoQueue;

abstract public class ScoutPubSubService extends AbstractPubSub<CRDTIdentifier> implements SwiftSubscriber {

    final Endpoint suPubSub;
    final RpcEndpoint endpoint;

    final Set<CRDTIdentifier> unsubscriptions = Collections.synchronizedSet(new HashSet<CRDTIdentifier>());

    final Task updater;
    final FifoQueue<SwiftNotification> fifoQueue;
    private MetadataStatsCollector statsCollector;
    private boolean disasterSafeSession;

    public ScoutPubSubService(final String clientId, boolean disasterSafeSession, final Endpoint surrogate,
            final MetadataStatsCollector statsCollector) {
        super(clientId);

        this.disasterSafeSession = disasterSafeSession;
        this.statsCollector = statsCollector;

        // process incoming events observing source fifo order...
        this.fifoQueue = new FifoQueue<SwiftNotification>() {
            public void process(SwiftNotification event) {
                event.payload().notifyTo(ScoutPubSubService.this);
            }
        };

        this.suPubSub = Networking.resolve(surrogate.getHost(), DCConstants.PUBSUB_PORT);
        this.endpoint = Networking.rpcConnect().toService(0, new SwiftProtocolHandler() {
            @Override
            public void onReceive(RpcHandle conn, SwiftNotification evt) {
                fifoQueue.offer(evt.seqN(), evt);
            }
        });

        this.endpoint.send(suPubSub, new PubSubHandshake(clientId, disasterSafeSession));

        updater = new Task(5) {
            public void run() {
                updateSurrogatePubSub();
            }
        };
    }

    private void updateSurrogatePubSub() {
        final Set<CRDTIdentifier> uset = new HashSet<CRDTIdentifier>(unsubscriptions);
        final UnsubscribeUpdatesRequest request = new UnsubscribeUpdatesRequest(-1L, super.id(), disasterSafeSession,
                uset);
        request.recordMetadataSample(statsCollector);
        endpoint.send(suPubSub, request, new SwiftProtocolHandler() {
            public void onReceive(RpcHandle conn, UnsubscribeUpdatesReply ack) {
                unsubscriptions.removeAll(uset);
                ack.recordMetadataSample(statsCollector);
            }
        }, 0);
    }

    // TODO Q: no synchronization required for these two methods??
    public void subscribe(CRDTIdentifier key) {
        unsubscriptions.remove(key);
        super.subscribe(key, this);
    }

    public void unsubscribe(CRDTIdentifier key) {
        if (super.unsubscribe(key, this)) {
            unsubscriptions.add(key);
            if (!updater.isScheduled())
                updater.reSchedule(0.1);
        }
    }

    public SortedSet<CRDTIdentifier> keys() {
        return new TreeSet<CRDTIdentifier>(super.subscribers.keySet());
    }

    @Override
    public void onNotification(Notifyable<CRDTIdentifier> event) {
        Thread.dumpStack();
    }

    @Override
    public void onNotification(SwiftNotification event) {
        Thread.dumpStack();
    }
}
