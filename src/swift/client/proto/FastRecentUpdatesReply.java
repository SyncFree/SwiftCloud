package swift.client.proto;

import java.util.List;
import java.util.Map;

import swift.clocks.CausalityClock;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDTOperation;
import swift.crdt.operations.CRDTObjectOperationsGroup;
import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * Server reply to fast recent updates request, a summary of all subscription changes
 * and updates since the last message.
 * Unlike RecentUpdatesReply, this method returns a best effort result, which means
 * that for some objects the result may not include all known updates at the server.
 * Note that it will always include a prefix of updates.
 * 
 * @author mzawirski
 */
public class FastRecentUpdatesReply implements RpcMessage {
    public enum SubscriptionStatus {
        /**
         * Subscriptions active at the time of last communication specified in
         * the request ({@link FastRecentUpdatesRequest#getLastClock()}) are still
         * active.
         */
        ACTIVE,
        /**
         * Previous subscriptions are lost due to timeout or lost message,
         * client should renew all subscriptions.
         */
        LOST
        // TODO(mzawirski): a more involved protocol could distinguish these two
        // cases and deal with them more efficiently; let's keep it as a
        // possible optimization
    }
    
    public static class SubscriptionInfo
    {
        CRDTIdentifier id;
        /**
         * Previous clock of the CRDT
         */
        CausalityClock oldClock;
        /**
         * Current clock of the CRDT after submitting this changes
         */
        CausalityClock newClock;
        protected List<CRDTOperation<?>> updates;
        
        public SubscriptionInfo(CRDTIdentifier id, CausalityClock oldClock, CausalityClock newClock, List<CRDTOperation<?>> updates) {
            this.id = id;
            this.oldClock = oldClock;
            this.newClock = newClock;
            this.updates = updates;
        }
    }
    
    protected SubscriptionStatus status;
    protected Map<CRDTIdentifier,SubscriptionInfo> subscriptions;

    /**
     * No-args constructor for Kryo-serialization.
     */
    public FastRecentUpdatesReply() {
    }

    public FastRecentUpdatesReply(SubscriptionStatus status,
            Map<CRDTIdentifier, SubscriptionInfo> subscriptions) {
        this.status = status;
        this.subscriptions = subscriptions; 
    }

    /**
     * @return status of subscription
     */
    public SubscriptionStatus getStatus() {
        return status;
    }

    /**
     * @return Information for active subscriptions since the last message, triggered by
     *         {@link FetchObjectVersionRequest#isSubscribeUpdatesRequest()};
     *         map of object identifier to the information of subscriptions;
     *         meaningless if status is {@link SubscriptionStatus#LOST}
     */
    public Map<CRDTIdentifier, SubscriptionInfo> getSubscriptions() {
        return subscriptions;
    }

    @Override
    public void deliverTo(RpcConnection conn, RpcHandler handler) {
        ((RecentUpdatesReplyHandler) handler).onReceive(conn, this);
    }
}
