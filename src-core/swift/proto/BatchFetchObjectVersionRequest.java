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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
 * Client request to fetch a version of an object or a number of objects
 * (batch).
 * 
 * @author mzawirski
 */
public class BatchFetchObjectVersionRequest extends ClientRequest implements MetadataSamplable, KryoSerializable {
    protected List<CRDTIdentifier> uids;
    protected CausalityClock version;
    protected boolean sendMoreRecentUpdates;
    protected boolean subscribe;
    protected boolean sendDCVector;

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    BatchFetchObjectVersionRequest() {
    }

    public BatchFetchObjectVersionRequest(String clientId, boolean disasterSafe, CausalityClock version,
            final boolean sendMoreRecentUpdates, boolean subscribe, boolean sendDCVersion, CRDTIdentifier... uids) {
        super(clientId, disasterSafe);
        this.uids = Arrays.asList(uids);
        this.version = version;
        this.subscribe = subscribe;
        this.sendMoreRecentUpdates = sendMoreRecentUpdates;
        this.sendDCVector = sendDCVersion;
    }

    public boolean isSendDCVector() {
        return sendDCVector;
    }

    public boolean hasSubscription() {
        return subscribe;
    }

    public int getBatchSize() {
        return uids.size();
    }

    /**
     * @return id of the requested object number 0 <= idx < getBatchSize()
     */
    public CRDTIdentifier getUid(final int idx) {
        return uids.get(idx);
    }

    /**
     * @return ids of the requested objects
     */
    public List<CRDTIdentifier> getUids() {
        return uids;
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

        collector.recordStats(this, totalSize, 0, 0, globalMetadata, uids.size(), version.getSize(),
                version.getExceptionsNumber());
    }

    @Override
    public void write(Kryo kryo, Output output) {
        baseWrite(kryo, output);
        output.writeVarInt(uids.size(), true);
        for (final CRDTIdentifier uid : uids) {
            uid.write(kryo, output);
        }
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
        final int batchSize = input.readVarInt(true);
        uids = new ArrayList<CRDTIdentifier>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            final CRDTIdentifier uid = new CRDTIdentifier();
            uid.read(kryo, input);
            uids.add(uid);
        }
        version = new VersionVectorWithExceptions();
        ((VersionVectorWithExceptions) version).read(kryo, input);
        final byte options = input.readByte();
        sendMoreRecentUpdates = (options & 1) != 0;
        subscribe = (options & (1 << 1)) != 0;
        sendDCVector = (options & (1 << 2)) != 0;
    }
}
