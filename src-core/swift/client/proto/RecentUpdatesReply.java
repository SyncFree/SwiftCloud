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

    /**
     * @param status
     *            subscription status; when {@link SubscriptionStatus#LOST} all
     *            other arguments are meaningless
     * @param objectsPreviousClocks
     *            map from object identifier to the previous version of an
     *            object, indicating that updates since that version must be
     *            included in this message; there is an entry for each object
     *            that is in the subscription set of this session; for most
     *            objects the old version is expected to be the
     *            {@link #getClock()} of the preceding notification message
     *            (using the same clock instance is encouraged as a space
     *            optimization) except for the object just added to subscription
     *            set where it points to the object version sent in the fetch
     *            reply; the map can be empty or null, but cannot point to a
     *            null value
     * @param updates
     *            a sequence of all updates since objectsPreviousClocks old
     *            versions and a clock for all objects in objectsPreviousClocks
     *            key-set; a subsequence (projection) of this updates on object
     *            X must be a valid linear extension of updates on X; note that
     *            updates can use shared dependency clock to save space (see
     *            {@link CRDTObjectUpdatesGroup#withDependencyClock(CausalityClock)}
     *            ); the list can be empty or null
     * @param clock
     *            new version of all subscribed objects, indicating that prior
     *            updates since the old version are included in this message
     */
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
     * @return map from object identifier to the previous version of an object,
     *         indicating that updates since that version must be included in
     *         this message; there is an entry for each object that is in the
     *         subscription set of this session; for most objects the old
     *         version is expected to be the {@link #getClock()} of the
     *         preceding notification message except for the object just added
     *         to subscription set where it points to the object version sent in
     *         the fetch reply; the map can be empty or null, but cannot point
     *         to a null value; meaningless if status is
     *         {@link SubscriptionStatus#LOST}
     */
    public Map<CRDTIdentifier, CausalityClock> getObjectsPreviousClocks() {
        return objectsPreviousClocks;
    }

    /**
     * @return true if this session has any active subscriptions
     */
    public boolean hasActiveSubscriptions() {
        return objectsPreviousClocks != null && !objectsPreviousClocks.isEmpty();
    }

    /**
     * @return new version of all subscribed objects, indicating that prior
     *         updates since the old version are included in this message;
     *         meaningless if status is {@link SubscriptionStatus#LOST}
     */
    public CausalityClock getClock() {
        return clock;
    }

    /**
     * @return a sequence of all updates between
     *         {@link #getObjectsPreviousClocks()} old versions and
     *         {@link #getClock()} for all subscribed objects; a subsequence
     *         (projection) of this updates on object X must be a valid linear
     *         extension of updates on X; the list can be empty or null
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
