package swift.pubsub;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import swift.crdt.CRDTIdentifier;
import swift.proto.SwiftProtocolHandler;
import swift.proto.UnsubscribeUpdatesReply;
import swift.proto.UnsubscribeUpdatesRequest;
import sys.RpcServices;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandle;
import sys.pubsub.PubSubNotification;
import sys.pubsub.impl.AbstractPubSub;
import sys.scheduler.Task;
import sys.utils.FifoQueue;

public class ScoutPubSubService extends AbstractPubSub<CRDTIdentifier> implements SwiftSubscriber {

    final String clientId;
    final Endpoint surrogate;
    final RpcEndpoint endpoint;

    final FifoQueue<PubSubNotification> fifoQueue;

    final Set<CRDTIdentifier> unsubscriptions = Collections.synchronizedSet(new HashSet<CRDTIdentifier>());

    final Task updater;

    public ScoutPubSubService(final String clientId, final RpcEndpoint scoutEndpoint, final Endpoint surrogate) {
        this.clientId = clientId;
        this.surrogate = surrogate;

        this.endpoint = scoutEndpoint.getFactory().toService(RpcServices.PUBSUB.ordinal(), new SwiftProtocolHandler() {
            @Override
            public void onReceive(RpcHandle conn, PubSubNotification update) {
                fifoQueue.offer(update.seqN(), update);
            }
        });

        this.fifoQueue = new FifoQueue<PubSubNotification>() {
            public void process(PubSubNotification p) {
                p.payload().notifyTo(ScoutPubSubService.this);
            }
        };

        updater = new Task(0) {
            public void run() {
                final Set<CRDTIdentifier> uset = new HashSet<CRDTIdentifier>(unsubscriptions);
                scoutEndpoint.send(surrogate, new UnsubscribeUpdatesRequest(-1L, clientId, uset),
                        new SwiftProtocolHandler() {
                            public void onReceive(RpcHandle conn, UnsubscribeUpdatesReply ack) {
                                unsubscriptions.removeAll(uset);
                            }
                        }, 0);
            }
        };
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

    @Override
    public void onNotification(UpdateNotification update) {
        Thread.dumpStack();
    }

    @Override
    public void onNotification(SnapshotNotification snapshot) {
        Thread.dumpStack();
    }
}
