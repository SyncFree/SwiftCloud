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

import java.util.List;
import java.util.Map;

import swift.clocks.CausalityClock;
import swift.crdt.CRDTIdentifier;
import swift.crdt.operations.CRDTObjectUpdatesGroup;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * Server reply to recent updates request, a summary of all subscription changes
 * and updates since the last message. This message includes all the updates
 * since the {@link #getObjectsPreviousClocks()} (specified for each object) and
 * the new {@link #getClock()}, and estimated values of committed clocks.
 * 
 * @author mzawirski
 */
public class RecentUpdatesReply implements RpcMessage {
    public enum SubscriptionStatus {
        /**
         * Subscriptions active at the time of last communication specified in
         * the request ({@link RecentUpdatesRequest#getLastClock()}) are still
         * active.
         */
        ACTIVE,
        /**
         * Previous subscriptions are lost due to timeout or lost message,
         * client should renew all subscriptions.
         */
        LOST
        // TODO(mzawirski): a more involved protocol could distinguish these two
        // cases and deal with lost messages more efficiently; let's keep it as
        // a possible optimization
        // TODO(mzawirski): the status is actually not needed for this simple
        // version of protocol that includes all objects from subscription set
        // in each message.
    }

    protected SubscriptionStatus status;
    protected Map<CRDTIdentifier, CausalityClock> objectsPreviousClocks;
    protected List<CRDTObjectUpdatesGroup> updates;
    protected CausalityClock clock;
    protected CausalityClock estimatedCommittedVersion;
    protected CausalityClock estimatedDistasterDurableCommittedVersion;

    /**
     * No-args constructor for Kryo-serialization.
     */
    RecentUpdatesReply() {
    }

    public RecentUpdatesReply(SubscriptionStatus status, Map<CRDTIdentifier, CausalityClock> objectsPreviousClocks,
            List<CRDTObjectUpdatesGroup> updates, CausalityClock clock, CausalityClock estimatedCommittedVersion,
            CausalityClock estimatedDistasterDurableCommittedVersion) {
        this.status = status;
        this.objectsPreviousClocks = objectsPreviousClocks;
        this.updates = updates;
        this.clock = clock;
        this.estimatedCommittedVersion = estimatedCommittedVersion;
        this.estimatedDistasterDurableCommittedVersion = estimatedDistasterDurableCommittedVersion;
    }

    /**
     * @return status of subscription
     */
    public SubscriptionStatus getStatus() {
        return status;
    }

    /**
     * @return a map from object identifier to clock; there is an entry for each
     *         object that is in the subscription set of this session to the old
     *         object version; this message is guaranteed to contain all updates
     *         since this version until {@link #getClock()}; meaningless if
     *         status is {@link SubscriptionStatus#LOST}
     */
    public Map<CRDTIdentifier, CausalityClock> getObjectsPreviousClocks() {
        return objectsPreviousClocks;
    }

    /**
     * @return true if this notification confirms any new subscription
     * @see #getNewlyConfirmedSubscriptions()
     */
    public boolean hasActiveSubscriptions() {
        return objectsPreviousClocks != null && !objectsPreviousClocks.isEmpty();
    }

    /**
     * @return the latest clock for which every update on subscribed objects was
     *         sent; meaningless if status is {@link SubscriptionStatus#LOST}
     */
    public CausalityClock getClock() {
        return clock;
    }

    /**
     * @return list of updates on subscribed objects; null in case of no
     *         updates; the order in the list is a linear extension of the
     *         causal order; meaningless if status is
     *         {@link SubscriptionStatus#LOST}
     */
    public List<CRDTObjectUpdatesGroup> getUpdates() {
        return updates;
    }

    /**
     * @return true if the message contains any updates
     */
    public boolean hasUpdates() {
        return updates != null && !updates.isEmpty();
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
        ((RecentUpdatesReplyHandler) handler).onReceive(conn, this);
    }
}
