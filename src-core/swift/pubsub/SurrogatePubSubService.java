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
import sys.pubsub.impl.AbstractPubSub;

public class SurrogatePubSubService extends AbstractPubSub<CRDTIdentifier> {
    static Logger logger = Logger.getLogger(DCSurrogate.class.getName());

    final Executor executor;
    final RpcEndpoint endpoint;
    final SwiftProtocolHandler handler;
    final DCSurrogate surrogate;

    final FifoQueues fifoQueues = new FifoQueues();
    final CausalityClock minDcVersion = ClockFactory.newClock();
    final Map<String, CausalityClock> versions = new ConcurrentHashMap<String, CausalityClock>();

    public SurrogatePubSubService(Executor executor, final DCSurrogate surrogate) {
        super(surrogate.getId());

        this.executor = executor;
        this.surrogate = surrogate;
        this.handler = new SwiftProtocolHandler() {

            @Override
            public void onReceive(RpcHandle conn, SwiftNotification evt) {
                if (logger.isLoggable(Level.INFO)) {
                    logger.info("SwiftNotification payload = " + evt.payload());
                }
                fifoQueues.queueFor(evt.src(), new SwiftProtocolHandler() {
                    public void onReceive(RpcHandle nil, SwiftNotification evt2) {

                        final String surrogateId = evt2.timestamp().getIdentifier();
                        updateDcVersions(surrogateId, evt2.dcVersion);

                        evt2.payload().notifyTo(SurrogatePubSubService.this);
                    }
                }).offer(evt.seqN(), evt);
            }

            @Override
            public void onReceive(RpcHandle conn, PubSubHandshake request) {
                if (logger.isLoggable(Level.INFO)) {
                    logger.info("PubSubHandshake client = " + request.getClientId() + " @ " + conn.remoteEndpoint());
                }
                logger.info("##### PubSubHandshake client = " + request.getClientId() + " @ " + conn.remoteEndpoint());
                surrogate.getSession(request.getClientId()).setClientEndpoint(conn.remoteEndpoint());
            }

            @Override
            public void onReceive(RpcHandle handle, UnsubscribeUpdatesRequest request) {
                if (logger.isLoggable(Level.INFO)) {
                    logger.info("UnsubscribeUpdatesRequest client = " + request.getClientId());
                }
                surrogate.getSession(request.getClientId()).unsubscribe(request.getUnSubscriptions());
                handle.reply(new UnsubscribeUpdatesReply(request.getId()));
            }
        };
        this.endpoint = Networking.rpcBind(DCConstants.PUBSUB_PORT).toService(0, handler);
    }

    public RpcEndpoint endpoint() {
        return endpoint;
    }

    public synchronized CausalityClock minDcVersion() {
        return minDcVersion.clone();
    }

    synchronized public void updateDcVersions(String srcId, CausalityClock estimate) {
        if (!srcId.equals(surrogate.getId()))
            versions.put(srcId, estimate);

        CausalityClock tmp = surrogate.getEstimatedDCVersionCopy();
        for (CausalityClock cc : versions.values())
            tmp.intersect(cc);

        minDcVersion.merge(tmp);
    }
}
