/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package swift.client.proto;

import java.util.ArrayList;
import java.util.List;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.operations.CRDTObjectUpdatesGroup;
import sys.net.api.rpc.RpcHandle;
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
// TODO: We really need to encode the clocks more efficiently, perhaps using
// common intersection and deltas.
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
        protected List<CRDTObjectUpdatesGroup<?>> updates;
        protected CausalityClock pruneClock;

        public ObjectSubscriptionInfo() {
        }

        public ObjectSubscriptionInfo(CRDTIdentifier id, CausalityClock oldClock, CausalityClock newClock,
                CausalityClock pruneClock, CRDTObjectUpdatesGroup<?> update, long updateCounter) {
            this.id = id;
            this.oldClock = oldClock;
            this.newClock = newClock;
            this.updates = new ArrayList<CRDTObjectUpdatesGroup<?>>();
            updates.add(update);
            this.dirty = true;
            this.pruneClock = pruneClock;
        }

        public ObjectSubscriptionInfo(CRDTIdentifier id, CausalityClock oldClock, CausalityClock newClock,
                CausalityClock pruneClock, List<CRDTObjectUpdatesGroup<?>> updates, boolean dirty, long updateCounter) {
            this.id = id;
            this.oldClock = oldClock;
            this.newClock = newClock;
            this.updates = new ArrayList<CRDTObjectUpdatesGroup<?>>();
            if (updates != null)
                this.updates.addAll(updates);
            this.dirty = dirty;
            this.pruneClock = pruneClock;
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
        public List<CRDTObjectUpdatesGroup<?>> getUpdates() {
            return updates;
        }

        public void clearOperations() {
            updates.clear();
        }

        public ObjectSubscriptionInfo clone() {
            return new ObjectSubscriptionInfo(id, oldClock, newClock, pruneClock, updates, dirty, -1);
        }

        public ObjectSubscriptionInfo cloneNotification() {
            return new ObjectSubscriptionInfo(id, oldClock, newClock, pruneClock, null, dirty, -1);
        }

        public ObjectSubscriptionInfo clone(Timestamp t) {
            CausalityClock newC = newClock.clone();
            if (t != null)
                newC.recordAllUntil(t);
            return new ObjectSubscriptionInfo(id, oldClock, newC, pruneClock, updates, dirty, -1);
        }

        public CausalityClock getPruneClock() {
            return pruneClock;
        }

        // public long getUpdateCounter() {
        // return updateCounter;
        // }
    }

    protected SubscriptionStatus status;
    protected List<ObjectSubscriptionInfo> subscriptions;
    protected CausalityClock estimatedCommittedVersion;
    protected CausalityClock estimatedDistasterDurableCommittedVersion;

    public int seqN;

    /**
     * No-args constructor for Kryo-serialization.
     */
    FastRecentUpdatesReply() {
    }

    public FastRecentUpdatesReply(SubscriptionStatus status, List<ObjectSubscriptionInfo> subscriptions,
            CausalityClock estimatedCommittedVersion, CausalityClock estimatedDistasterDurableCommittedVersion, int seqN) {
        this.status = status;
        this.subscriptions = subscriptions;
        this.estimatedCommittedVersion = estimatedCommittedVersion;
        this.estimatedDistasterDurableCommittedVersion = estimatedDistasterDurableCommittedVersion;
        this.estimatedDistasterDurableCommittedVersion.intersect(estimatedCommittedVersion);

        this.seqN = seqN;
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
    public CausalityClock getEstimatedCommittedVersion() {
        return estimatedCommittedVersion;
    }

    /**
     * @return estimation of the latest committed clock in the store, durable in
     *         even in case of disaster affecting fragment of the store
     */
    public CausalityClock getEstimatedDisasterDurableCommittedVersion() {
        return estimatedDistasterDurableCommittedVersion;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((FastRecentUpdatesReplyHandler) handler).onReceive(conn, this);
    }
}
