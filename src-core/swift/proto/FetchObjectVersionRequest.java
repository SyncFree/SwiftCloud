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
package swift.proto;

import swift.clocks.CausalityClock;
import swift.clocks.VersionVectorWithExceptions;
import swift.crdt.core.CRDTIdentifier;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Client request to fetch a particular version of an object.
 * 
 * @author mzawirski
 */
public class FetchObjectVersionRequest extends ClientRequest implements MetadataSamplable, KryoSerializable {
    protected CRDTIdentifier uid;
    // TODO: could be derived from client's session?
    protected CausalityClock version;
    // FIXME: make these things optional? Used only by evaluation.
    // protected CausalityClock clock;
    // protected CausalityClock disasterDurableClock;
    protected boolean sendMoreRecentUpdates;
    protected boolean subscribe;
    protected boolean sendDCVector;

    public transient RpcHandle replyHandle;

    // public long timestamp = sys.Sys.Sys.timeMillis(); // FOR EVALUATION, TO
    // BE REMOVED...

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    FetchObjectVersionRequest() {
    }

    public FetchObjectVersionRequest(String clientId, boolean disasterSafe, CRDTIdentifier uid, CausalityClock version,
            final boolean sendMoreRecentUpdates, boolean subscribe, boolean sendDCVersion) {
        super(clientId, disasterSafe);
        this.uid = uid;
        this.version = version;
        this.subscribe = subscribe;
        this.sendMoreRecentUpdates = sendMoreRecentUpdates;
        this.sendDCVector = sendDCVersion;
    }

    // public FetchObjectVersionRequest(String clientId, CRDTIdentifier uid,
    // CausalityClock version,
    // final boolean strictUnprunedVersion, CausalityClock clock, CausalityClock
    // disasterDurableClock,
    // boolean subscribe) {
    // super(clientId);
    // this.uid = uid;
    // this.clock = clock;
    // this.version = version;
    // this.subscribe = subscribe;
    // this.strictUnprunedVersion = strictUnprunedVersion;
    // this.disasterDurableClock = disasterDurableClock;
    // }

    public boolean isSendDCVector() {
        return sendDCVector;
    }

    public boolean hasSubscription() {
        return subscribe;
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
     * @return true if more recent updates than the requested version should be
     *         send in the reply
     */
    public boolean isSendMoreRecentUpdates() {
        return sendMoreRecentUpdates;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((SwiftProtocolHandler) handler).onReceive(conn, this);
    }

    /**
     * @return latest known clock in the store, i.e. valid snapshot point
     *         candidate
     */
    public CausalityClock getClock() {
        return null;
        // return clock;
    }

    /**
     * @return latest known clock in the store, i.e. valid snapshot point
     *         candidate, durable in even in case of disaster affecting fragment
     *         of the store
     */
    public CausalityClock getDistasterDurableClock() {
        return null;
        // return disasterDurableClock;
    }

    public void setHandle(RpcHandle replyHandle) {
        this.replyHandle = replyHandle;
    }

    @Override
    public void recordMetadataSample(MetadataStatsCollector collector) {
        if (!collector.isEnabled()) {
            return;
        }
        Kryo kryo = collector.getFreshKryo();
        Output buffer = collector.getFreshKryoBuffer();

        // TODO: capture from the wire, rather than recompute here
        kryo.writeObject(buffer, this);
        final int totalSize = buffer.position();

        kryo = collector.getFreshKryo();
        buffer = collector.getFreshKryoBuffer();
        kryo.writeObject(buffer, version);
        final int globalMetadata = buffer.position();

        collector.recordStats(this, totalSize, 0, 0, globalMetadata, 1, version.getSize(),
                version.getExceptionsNumber());
    }

    @Override
    public void write(Kryo kryo, Output output) {
        baseWrite(kryo, output);
        uid.write(kryo, output);
        ((VersionVectorWithExceptions) version).write(kryo, output);
        byte options = 0;
        if (sendMoreRecentUpdates) {
            options |= 1;
        }
        if (subscribe) {
            options |= 1 << 1;
        }
        if (sendDCVector) {
            options |= 1 << 2;
        }
        output.writeByte(options);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        baseRead(kryo, input);
        uid = new CRDTIdentifier();
        uid.read(kryo, input);
        version = new VersionVectorWithExceptions();
        ((VersionVectorWithExceptions) version).read(kryo, input);
        final byte options = input.readByte();
        sendMoreRecentUpdates = (options & 1) != 0;
        subscribe = (options & (1 << 1)) != 0;
        sendDCVector = (options & (1 << 2)) != 0;
    }
}
