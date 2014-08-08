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

import java.util.List;

import swift.clocks.CausalityClock;
import swift.clocks.VersionVectorWithExceptions;
import swift.crdt.core.CRDT;
import swift.crdt.core.ManagedCRDT;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Server reply to version fetch request for a number of objects. The order of
 * elements in the reply may corresponds to the order of elements in
 * {@link BatchFetchObjectVersionRequest}.
 * 
 * @author mzawirski
 */
public class BatchFetchObjectVersionReply implements RpcMessage, MetadataSamplable, KryoSerializable {
    public enum FetchStatus {
        /**
         * The reply contains requested version.
         */
        OK,
        /**
         * The requested object is not in the store.
         */
        OBJECT_NOT_FOUND,
        /**
         * The requested version of an object is available in the store.
         */
        VERSION_MISSING,
        /**
         * The requested version of an object was pruned from the store.
         */
        VERSION_PRUNED,
        /**
         * 
         */
        UP_TO_DATE
    }

    protected FetchStatus[] statuses;
    protected ManagedCRDT[] crdts;
    protected CausalityClock estimatedLatestKnownClock;
    protected CausalityClock estimatedDisasterDurableLatestKnownClock;
    private transient int compressionReferenceIdx = -1;

    // public Map<String, Object> staleReadsInfo;

    // Fake constructor for Kryo serialization. Do NOT use.
    BatchFetchObjectVersionReply() {
    }

    public BatchFetchObjectVersionReply(final int batchSize, CausalityClock estimatedLatestKnownClock,
            CausalityClock estimatedDisasterDurableLatestKnownClock) {
        this.crdts = new ManagedCRDT[batchSize];
        this.statuses = new FetchStatus[batchSize];
        this.estimatedLatestKnownClock = estimatedLatestKnownClock;
        this.estimatedDisasterDurableLatestKnownClock = estimatedDisasterDurableLatestKnownClock;
        if (estimatedLatestKnownClock != null && estimatedDisasterDurableLatestKnownClock != null) {
            // TODO: use diff over here?
            this.estimatedDisasterDurableLatestKnownClock.intersect(estimatedLatestKnownClock);
        }
    }

    public void setReply(final int index, final FetchStatus status, final ManagedCRDT crdt) {
        synchronized (this) {
            statuses[index] = status;
            crdts[index] = crdt;
        }
    }

    // public FetchObjectVersionReply(FetchStatus status, ManagedCRDT<?> crdt,
    // CausalityClock estimatedLatestKnownClock,
    // CausalityClock estimatedDisasterDurableLatestKnownClock, Map<String,
    // Object> staleReadsInfo) {
    //
    // this(status, crdt, estimatedLatestKnownClock,
    // estimatedDisasterDurableLatestKnownClock);
    //
    // // EVALUATION
    // this.staleReadsInfo = staleReadsInfo;
    // }

    public int getBatchSize() {
        return statuses.length;
    }

    /**
     * @return status code of the reply identified by idx, where 0 <= idx <
     *         getBatchSize()
     */
    public FetchStatus getStatus(final int idx) {
        return statuses[idx];
    }

    /**
     * @return state of an object requested identified by idx, where 0 <= idx <
     *         getBatchSize(); null if {@link #getStatus()} is
     *         {@link FetchStatus#OBJECT_NOT_FOUND}.
     */
    // Old docs, not true anymore: if {@link #getStatus()} is {@link
    // FetchStatus#OK} then the object is
    // pruned from history at most to the level specified by version in
    // the original client request;
    public ManagedCRDT<?> getCrdt(int idx) {
        return crdts[idx];
    }

    /**
     * @return estimation of the latest committed clock in the store
     */
    public CausalityClock getEstimatedCommittedVersion() {
        return estimatedLatestKnownClock;
    }

    /**
     * @return estimation of the latest committed clock in the store, durable
     *         even in case of distaster affecting fragment of the store
     */
    public CausalityClock getEstimatedDisasterDurableCommittedVersion() {
        return estimatedDisasterDurableLatestKnownClock;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        if (handler != RpcHandler.NONE)
            ((SwiftProtocolHandler) handler).onReceive(conn, this);
    }

    public void compressAllOKReplies(CausalityClock commonPruneClock, CausalityClock commonClock) {
        // clear all but first pair of clocks - that one will act as a
        // reference clock for all objects in the batch.
        boolean firstOkFound = false;
        for (int i = 0; i < getBatchSize(); i++) {
            if (statuses[i] == FetchStatus.OK) {
                if (firstOkFound) {
                    crdts[i].forceSetClocks(null, null);
                } else {
                    firstOkFound = true;
                    crdts[i].forceSetClocks(commonPruneClock, commonClock);
                }
            }
        }
    }

    @Override
    public void recordMetadataSample(MetadataStatsCollector collector) {
        if (!collector.isMessageReportEnabled()) {
            return;
        }
        Kryo kryo = collector.getFreshKryo();
        Output buffer = collector.getFreshKryoBuffer();

        kryo.writeObject(buffer, this);
        final int totalSize = buffer.position();

        kryo = collector.getFreshKryo();
        buffer = collector.getFreshKryoBuffer();

        int maxExceptionsNum = 0;
        int maxVectorSize = 0;
        if (estimatedDisasterDurableLatestKnownClock != null) {
            maxExceptionsNum = Math.max(estimatedDisasterDurableLatestKnownClock.getExceptionsNumber(),
                    maxExceptionsNum);
            maxVectorSize = Math.max(estimatedDisasterDurableLatestKnownClock.getSize(), maxVectorSize);
        }
        if (estimatedLatestKnownClock != null) {
            maxExceptionsNum = Math.max(estimatedLatestKnownClock.getExceptionsNumber(), maxExceptionsNum);
            maxVectorSize = Math.max(estimatedLatestKnownClock.getSize(), maxVectorSize);
        }

        int versionSize = 0;
        int valueSize = 0;
        for (final ManagedCRDT crdt : crdts) {
            if (crdt != null) {
                maxExceptionsNum = Math.max(crdt.getClock().getExceptionsNumber(), maxExceptionsNum);

                kryo = collector.getFreshKryo();
                buffer = collector.getFreshKryoBuffer();
                kryo.writeObject(buffer, crdt.getUID());
                // TODO: be more precise w.r.t version (we should get the
                // requested
                // version, not the latest)
                final CRDT version = crdt.getLatestVersion(null);
                kryo.writeObject(buffer, version);
                versionSize += buffer.position();

                kryo = collector.getFreshKryo();
                buffer = collector.getFreshKryoBuffer();
                kryo.writeObject(buffer, crdt.getUID());
                final Object value = version.getValue();
                if (value != null) {
                    kryo.writeObject(buffer, value);
                } else {
                    kryo.writeObject(buffer, false);
                }
                valueSize += buffer.position();
            }
        }

        kryo = collector.getFreshKryo();
        buffer = collector.getFreshKryoBuffer();
        if (estimatedDisasterDurableLatestKnownClock != null) {
            kryo.writeObject(buffer, estimatedDisasterDurableLatestKnownClock);
        }
        if (estimatedLatestKnownClock != null) {
            kryo.writeObject(buffer, estimatedLatestKnownClock);
        }
        int actualBatchSize = 0;
        for (int i = 0; i < getBatchSize(); i++) {
            if (statuses[i] != FetchStatus.UP_TO_DATE) {
                actualBatchSize++;
                kryo.writeObject(buffer, statuses[i]);
            }
            if (crdts[i] != null) {
                if (statuses[i] != FetchStatus.OK || compressionReferenceIdx < 0 || compressionReferenceIdx == i) {
                    kryo.writeObject(buffer, crdts[i].getClock());
                    kryo.writeObject(buffer, crdts[i].getPruneClock());
                    maxVectorSize = Math.max(crdts[i].getClock().getSize(), maxVectorSize);
                    maxVectorSize = Math.max(crdts[i].getPruneClock().getSize(), maxVectorSize);
                }
                final List log = crdts[i].getUpdatesTimestampMappingsSince(crdts[i].getPruneClock());
                if (!log.isEmpty()) {
                    kryo.writeObject(buffer, log);
                }
            }
        }
        final int globalMetadata = buffer.position();

        collector.recordMessageStats(this, totalSize, versionSize, valueSize, globalMetadata, getBatchSize(),
                actualBatchSize, actualBatchSize, maxVectorSize, maxExceptionsNum);
    }

    @Override
    public void write(Kryo kryo, Output output) {
        final int batchSize = getBatchSize();
        output.writeVarInt(batchSize, true);
        for (int i = 0; i < batchSize; i++) {
            output.writeVarInt(statuses[i].ordinal(), true);
            kryo.writeClassAndObject(output, crdts[i]);
        }
        kryo.writeObjectOrNull(output, estimatedLatestKnownClock, VersionVectorWithExceptions.class);
        kryo.writeObjectOrNull(output, estimatedDisasterDurableLatestKnownClock, VersionVectorWithExceptions.class);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        final int batchSize = input.readVarInt(true);
        statuses = new FetchStatus[batchSize];
        crdts = new ManagedCRDT[batchSize];
        compressionReferenceIdx = -1;
        for (int i = 0; i < batchSize; i++) {
            final int ordinal = input.readVarInt(true);
            // If ordinal is out range, then ArrayIndexOutOfBoundException
            // fires.
            statuses[i] = FetchStatus.values()[ordinal];
            crdts[i] = (ManagedCRDT) kryo.readClassAndObject(input);
            // Condition used by compressAllOKReplies():
            if (statuses[i] == FetchStatus.OK) {
                if (compressionReferenceIdx >= 0) {
                    // If clocks are compressed => decompress.
                    if (crdts[i].getClock() == null) {
                        crdts[i].forceSetClocks(crdts[compressionReferenceIdx].getPruneClock().clone(),
                                crdts[compressionReferenceIdx].getClock().clone());
                    }
                } else {
                    compressionReferenceIdx = i;
                }
            }
        }
        estimatedLatestKnownClock = kryo.readObjectOrNull(input, VersionVectorWithExceptions.class);
        estimatedDisasterDurableLatestKnownClock = kryo.readObjectOrNull(input, VersionVectorWithExceptions.class);
    }
}
