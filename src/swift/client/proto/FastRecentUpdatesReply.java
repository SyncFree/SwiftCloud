package swift.client.proto;

import java.util.ArrayList;
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

        public ObjectSubscriptionInfo() {
        }

        public ObjectSubscriptionInfo(CRDTIdentifier id, CausalityClock oldClock, CausalityClock newClock,
                CRDTObjectOperationsGroup<?> update) {
            this.id = id;
            this.oldClock = oldClock;
            this.newClock = newClock;
            this.updates = new ArrayList<CRDTObjectOperationsGroup<?>>();
            updates.add(update);
            this.dirty = true;
        }

        public ObjectSubscriptionInfo(CRDTIdentifier id, CausalityClock oldClock, CausalityClock newClock,
                List<CRDTObjectOperationsGroup<?>> updates, boolean dirty) {
            this.id = id;
            this.oldClock = oldClock;
            this.newClock = newClock;
            this.updates = new ArrayList<CRDTObjectOperationsGroup<?>>();
            if (updates != null)
                this.updates.addAll(updates);
            this.dirty = dirty;
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

        public void clearOperations() {
            updates.clear();
        }

        public ObjectSubscriptionInfo clone() {
            return new ObjectSubscriptionInfo(id, oldClock, newClock, updates, dirty);
        }
        public ObjectSubscriptionInfo cloneNotification() {
            return new ObjectSubscriptionInfo(id, oldClock, newClock, null, dirty);
        }
    }

    protected SubscriptionStatus status;
    protected List<ObjectSubscriptionInfo> subscriptions;
    protected CausalityClock estimatedLatestKnownClock;

    /**
     * No-args constructor for Kryo-serialization.
     */
    public FastRecentUpdatesReply() {
    }

    public FastRecentUpdatesReply( SubscriptionStatus status, List<ObjectSubscriptionInfo> subscriptions,
            CausalityClock estimatedLatestKnownClock) {
        this.subscriptions = subscriptions;
        this.estimatedLatestKnownClock = estimatedLatestKnownClock;
    }
    
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

    /**
     * @return estimation of the latest committed clock in the store
     */
    public CausalityClock getEstimatedLatestKnownClock() {
        return estimatedLatestKnownClock;
    }

    @Override
    public void deliverTo(RpcConnection conn, RpcHandler handler) {
        ((FastRecentUpdatesReplyHandler) handler).onReceive(conn, this);
    }
}
