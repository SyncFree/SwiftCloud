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

import swift.clocks.CausalityClock;
import swift.crdt.CRDTIdentifier;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

/**
 * Client request to fetch a particular version of an object.
 * 
 * @author mzawirski
 */
public class FetchObjectVersionRequest extends ClientRequest {
    protected CRDTIdentifier uid;
    protected CausalityClock version;
    protected CausalityClock clock;
    protected CausalityClock disasterDurableClock;

    protected boolean strictUnprunedVersion;
    protected SubscriptionType subscriptionType;

    public long timestamp = sys.Sys.Sys.timeMillis();

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    FetchObjectVersionRequest() {
    }

    public FetchObjectVersionRequest(String clientId, CRDTIdentifier uid, CausalityClock version,
            final boolean strictUnprunedVersion, SubscriptionType subscribeUpdates) {
        super(clientId);
        this.uid = uid;
        this.version = version;
        this.strictUnprunedVersion = strictUnprunedVersion;
        this.subscriptionType = subscribeUpdates;
    }

    public FetchObjectVersionRequest(String clientId, CRDTIdentifier uid, CausalityClock version,
            final boolean strictUnprunedVersion, SubscriptionType subscribeUpdates, CausalityClock clock,
            CausalityClock disasterDurableClock) {
        super(clientId);
        this.uid = uid;
        this.version = version;
        this.strictUnprunedVersion = strictUnprunedVersion;
        this.subscriptionType = subscribeUpdates;
        this.clock = clock;
        this.disasterDurableClock = disasterDurableClock;
    }

    /**
     * @return id of the requested object
     */
    public CRDTIdentifier getUid() {
        return uid;
    }

    /**
     * @return minimum version requested
     */
    public CausalityClock getVersion() {
        return version;
    }

    /**
     * @return true strictly this (unpruned) version needs to be available in
     *         the reply; otherwise a more recent version is acceptable
     */
    public boolean isStrictAvailableVersion() {
        return strictUnprunedVersion;
    }

    /**
     * @return the subscription type for the object
     */
    public SubscriptionType getSubscriptionType() {
        return subscriptionType;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((SwiftServer) handler).onReceive(conn, this);
    }

    /**
     * @return latest known clock in the store, i.e. valid snapshot point
     *         candidate
     */
    public CausalityClock getClock() {
        return clock;
    }

    /**
     * @return latest known clock in the store, i.e. valid snapshot point
     *         candidate, durable in even in case of disaster affecting fragment
     *         of the store
     */
    public CausalityClock getDistasterDurableClock() {
        return disasterDurableClock;
    }
}
