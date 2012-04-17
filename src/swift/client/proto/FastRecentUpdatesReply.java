package swift.client.proto;

import java.util.List;

import swift.clocks.CausalityClock;
import swift.crdt.CRDTIdentifier;
import swift.crdt.operations.CRDTObjectOperationsGroup;
import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * Server reply to fast recent updates request, a summary of all subscription
 * changes and updates since the last message. Unlike RecentUpdatesReply, this
 * method returns a best effort result, which means that for some objects the
 * result may not include all known updates at the server. Note that it will
 * always include a prefix of updates.
 * 
 * @author nmp, mzawirski
 */
public class FastRecentUpdatesReply implements RpcMessage {
    public enum SubscriptionStatus {
        /**
         * Subscriptions active at the time of last communication with the
         * client are still active.
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

    public static class ObjectSubscriptionInfo {
        protected CRDTIdentifier id;
        protected CausalityClock oldClock;
        protected CausalityClock newClock;
        protected boolean dirty;
        protected List<CRDTObjectOperationsGroup<?>> updates;

        public ObjectSubscriptionInfo(CRDTIdentifier id, CausalityClock oldClock, CausalityClock newClock,
                List<CRDTObjectOperationsGroup<?>> updates) {
            this.id = id;
            this.oldClock = oldClock;
            this.newClock = newClock;
            this.updates = updates;
        }

        /**
         * @return id of the object
         */
        public CRDTIdentifier getId() {
            return id;
        }

        /**
         * @return previous clock of the CRDT
         */
        public CausalityClock getOldClock() {
            return oldClock;
        }

        /**
         * @return current clock of the CRDT after submitting this changes
         */
        public CausalityClock getNewClock() {
            return newClock;
        }

        /**
         * @return true if there was any update to the object between
         *         {@link #getOldClock()} and {@link #getNewClock()}
         */
        public boolean isDirty() {
            return dirty;
        }

        /**
         * @return list of all updates to the object between
         *         {@link #getOldClock()} and {@link #getNewClock()} or null if
         *         only an invalidation (notification) has been requested
         */
        public List<CRDTObjectOperationsGroup<?>> getUpdates() {
            return updates;
        }
    }

    protected SubscriptionStatus status;
    protected List<ObjectSubscriptionInfo> subscriptions;

    /**
     * No-args constructor for Kryo-serialization.
     */
    public FastRecentUpdatesReply() {
    }

    public FastRecentUpdatesReply(SubscriptionStatus status, List<ObjectSubscriptionInfo> subscriptions) {
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
     * @return Information on updates for active subscriptions since the last
     *         message send by server, triggered by
     *         {@link FetchObjectVersionRequest#getSubscriptionType()}; list of
     *         information on subscriptions per object; meaningless if status is
     *         {@link SubscriptionStatus#LOST}
     */
    public List<ObjectSubscriptionInfo> getSubscriptions() {
        // TODO: let's clarify, is it for all active subscriptions or only for a
        // subset where we have some information available?
        return subscriptions;
    }

    @Override
    public void deliverTo(RpcConnection conn, RpcHandler handler) {
        ((RecentUpdatesReplyHandler) handler).onReceive(conn, this);
    }
}
