package swift.pubsub;

import static sys.net.api.Networking.Networking;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;

import swift.clocks.CausalityClock;
import swift.clocks.ClockFactory;
import swift.crdt.core.CRDTIdentifier;
import swift.dc.DCConstants;
import swift.dc.DCSurrogate;
import swift.proto.PubSubHandshake;
import swift.proto.SwiftProtocolHandler;
import swift.proto.UnsubscribeUpdatesReply;
import swift.proto.UnsubscribeUpdatesRequest;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandle;
import sys.pubsub.PubSubNotification;
import sys.pubsub.impl.AbstractPubSub;

public class SurrogatePubSubService extends AbstractPubSub<CRDTIdentifier> implements SwiftSubscriber {
    static Logger logger = Logger.getLogger(DCSurrogate.class.getName());

    final Executor executor;
    final RpcEndpoint endpoint;
    final SwiftProtocolHandler handler;
    final DCSurrogate surrogate;

    final FifoQueues fifoQueues = new FifoQueues();
    final CausalityClock minDcVersion = ClockFactory.newClock();
    final Map<Object, CausalityClock> versions = new ConcurrentHashMap<Object, CausalityClock>();

    public SurrogatePubSubService(Executor executor, final DCSurrogate surrogate) {
        super(surrogate.getId());

        this.executor = executor;
        this.surrogate = surrogate;
        this.handler = new SwiftProtocolHandler() {

            @Override
            public void onReceive(UpdateNotification evt) {
                if (logger.isLoggable(Level.INFO)) {
                    logger.info("SwiftNotification payload = " + evt.payload());
                }

                fifoQueues.queueFor(evt.src(), SurrogatePubSubService.this).offer(evt.seqN(), evt);
            }

            @Override
            public void onReceive(RpcHandle conn, PubSubHandshake request) {
                if (logger.isLoggable(Level.INFO)) {
                    logger.info("PubSubHandshake client = " + request.getClientId() + " @ " + conn.remoteEndpoint());
                }
                logger.info("##### PubSubHandshake client = " + request.getClientId() + " @ " + conn.remoteEndpoint());
                surrogate.getSession(request.getClientId(), request.isDisasterSafeSession()).setClientEndpoint(
                        conn.remoteEndpoint());
            }

            @Override
            public void onReceive(RpcHandle handle, UnsubscribeUpdatesRequest request) {
                if (logger.isLoggable(Level.INFO)) {
                    logger.info("UnsubscribeUpdatesRequest client = " + request.getClientId());
                }
                surrogate.getSession(request.getClientId(), request.isDisasterSafeSession()).unsubscribe(
                        request.getUnSubscriptions());
                handle.reply(new UnsubscribeUpdatesReply(request.getId()));
            }
        };
        this.endpoint = Networking.rpcBind(surrogate.pubsubPort).toService(0, handler);
    }

    public RpcEndpoint endpoint() {
        return endpoint;
    }

    public synchronized CausalityClock minDcVersion() {
        return minDcVersion.clone();
    }

    synchronized public void updateDcVersions(Object srcId, CausalityClock estimate) {
        if (!srcId.equals(surrogate.getId()))
            versions.put(srcId, estimate);

        CausalityClock tmp = surrogate.getEstimatedDCVersionCopy();
        for (CausalityClock cc : versions.values())
            tmp.intersect(cc);

        minDcVersion.merge(tmp);
    }

    synchronized public void onNotification(UpdateNotification update) {
        this.updateDcVersions(update.src(), update.dcVersion());
        super.publish(update);
    }

    @Override
    public void onNotification(BatchUpdatesNotification evt) {
        Thread.dumpStack();
    }

    @Override
    public void onNotification(PubSubNotification<CRDTIdentifier> evt) {
        Thread.dumpStack();
    }
}
