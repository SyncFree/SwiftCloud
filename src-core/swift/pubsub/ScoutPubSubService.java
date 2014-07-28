package swift.pubsub;

import static sys.net.api.Networking.Networking;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import swift.crdt.core.CRDTIdentifier;
import swift.proto.MetadataStatsCollector;
import swift.proto.PubSubHandshake;
import swift.proto.PubSubHandshakeReply;
import swift.proto.SwiftProtocolHandler;
import swift.proto.UnsubscribeUpdatesReply;
import swift.proto.UnsubscribeUpdatesRequest;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandle;
import sys.pubsub.PubSubNotification;
import sys.pubsub.impl.AbstractPubSub;
import sys.scheduler.Task;
import sys.utils.FifoQueue;

abstract public class ScoutPubSubService extends AbstractPubSub<CRDTIdentifier> implements SwiftSubscriber {
    public static final int HANDSHAKE_REPLY_DEADLINE_MS = 10000;
    private final static Object dummyVal = new Object();

    final Endpoint suPubSub;
    final RpcEndpoint endpoint;

    final Map<CRDTIdentifier, Object> unsubscriptions = new ConcurrentHashMap<CRDTIdentifier, Object>();

    final Task updater;
    private boolean disasterSafeSession;
    private MetadataStatsCollector statsCollector;
    final FifoQueue<PubSubNotification<CRDTIdentifier>> fifoQueue;

    public ScoutPubSubService(final String clientId, boolean disasterSafeSession, final Endpoint surrogate,
            final MetadataStatsCollector statsCollector) {
        super(clientId);

        this.disasterSafeSession = disasterSafeSession;
        this.statsCollector = statsCollector;

        // process incoming events observing source fifo order...
        this.fifoQueue = new FifoQueue<PubSubNotification<CRDTIdentifier>>() {
            public void process(PubSubNotification<CRDTIdentifier> event) {
                event.notifyTo(ScoutPubSubService.this);
            }
        };

        this.suPubSub = Networking.resolve(surrogate.getHost(), surrogate.getPort() + 1);
        this.endpoint = Networking.rpcConnect().toService(0, new SwiftProtocolHandler() {
            @Override
            public void onReceive(BatchUpdatesNotification evt) {
                fifoQueue.offer(evt.seqN(), evt);
            }
        });

        final AtomicReference<PubSubHandshakeReply> ref = new AtomicReference<PubSubHandshakeReply>(null);
        do {
            final PubSubHandshake handshakeReq = new PubSubHandshake(clientId, disasterSafeSession);
            this.endpoint.send(suPubSub, handshakeReq, new SwiftProtocolHandler() {
                public void onReceive(RpcHandle conn, PubSubHandshakeReply reply) {
                    ref.set(reply);
                }
            }, HANDSHAKE_REPLY_DEADLINE_MS);
        } while (ref.get() == null);

        updater = new Task(5) {
            public void run() {
                updateSurrogatePubSub();
            }
        };
    }

    private void updateSurrogatePubSub() {
        final Set<CRDTIdentifier> uset = new HashSet<CRDTIdentifier>(unsubscriptions.keySet());
        final UnsubscribeUpdatesRequest request = new UnsubscribeUpdatesRequest(-1L, super.id(), disasterSafeSession,
                uset);
        request.recordMetadataSample(statsCollector);
        endpoint.send(suPubSub, request, new SwiftProtocolHandler() {
            public void onReceive(RpcHandle conn, UnsubscribeUpdatesReply ack) {
                unsubscriptions.keySet().removeAll(uset);
                ack.recordMetadataSample(statsCollector);
            }
        }, 0);
    }

    public void subscribe(CRDTIdentifier key) {
        unsubscriptions.remove(key);
        super.subscribe(key, this);
    }

    public void unsubscribe(CRDTIdentifier key) {
        if (super.unsubscribe(key, this)) {
            unsubscriptions.put(key, dummyVal);
            if (!updater.isScheduled())
                updater.reSchedule(0.1);
        }
    }

    public SortedSet<CRDTIdentifier> keys() {
        return new TreeSet<CRDTIdentifier>(super.subscribers.keySet());
    }

    @Override
    public void onNotification(BatchUpdatesNotification evt) {
        Thread.dumpStack();
    }

    @Override
    public void onNotification(PubSubNotification<CRDTIdentifier> evt) {
        Thread.dumpStack();
    }

    @Override
    public void onNotification(UpdateNotification evt) {
        Thread.dumpStack();
    }
}
