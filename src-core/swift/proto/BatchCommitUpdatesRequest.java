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

import java.util.LinkedList;
import java.util.List;

import swift.crdt.core.CRDTObjectUpdatesGroup;
import swift.crdt.core.CRDTUpdate;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

/**
 * Client batch request to commit a a sequence of transactions. Transactions
 * should be committed sequentially and a depedency clock of <em>i+1</em>-th
 * should be updated to include system timestamp of <em>i</em>-th transaction.
 * 
 * @author mzawirski
 */
public class BatchCommitUpdatesRequest extends ClientRequest implements MetadataSamplable {
    protected LinkedList<CommitUpdatesRequest> commitRequests;

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    BatchCommitUpdatesRequest() {
    }

    /**
     * Creates a batch commit request made of provided list of requests.
     * 
     * @param clientId
     *            client id
     * @param commitRequests
     *            sequence of commit requests from the client
     */
    public BatchCommitUpdatesRequest(String clientId, boolean disasterSafeSession,
            List<CommitUpdatesRequest> commitRequests) {
        super(clientId, disasterSafeSession);
        this.commitRequests = new LinkedList<CommitUpdatesRequest>(commitRequests);
    }

    /**
     * @return sequence of commit updates requests, one possibly dependent on
     *         another
     */
    // TODO: Weaken the dependencies?
    public LinkedList<CommitUpdatesRequest> getCommitRequests() {
        return commitRequests;
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

        // TODO: get it from the wire, rather than recompute
        kryo.writeObject(buffer, this);
        final int totalSize = buffer.position();

        int maxExceptionsNum = 0;
        kryo = collector.getFreshKryo();
        buffer = collector.getFreshKryoBuffer();
        for (final CommitUpdatesRequest req : commitRequests) {
            for (final CRDTObjectUpdatesGroup<?> group : req.getObjectUpdateGroups()) {
                if (group.hasCreationState()) {
                    kryo.writeObject(buffer, group.getCreationState());
                }
                maxExceptionsNum = Math.max(group.getDependency().getExceptionsNumber(), maxExceptionsNum);
                kryo.writeObject(buffer, group.getTargetUID());
                kryo.writeObject(buffer, group.getOperations());
            }
        }
        final int updatesSize = buffer.position();

        kryo = collector.getFreshKryo();
        buffer = collector.getFreshKryoBuffer();
        for (final CommitUpdatesRequest req : commitRequests) {
            for (final CRDTObjectUpdatesGroup<?> group : req.getObjectUpdateGroups()) {
                if (group.hasCreationState()) {
                    final Object value = group.getCreationState().getValue();
                    if (value != null) {
                        kryo.writeObject(buffer, value);
                    } else {
                        kryo.writeObject(buffer, false);
                    }
                }
                kryo.writeObject(buffer, group.getTargetUID());
                for (final CRDTUpdate<?> op : group.getOperations()) {
                    if (op.getValueWithoutMetadata() != null) {
                        kryo.writeObject(buffer, op.getValueWithoutMetadata());
                    } else {
                        kryo.writeObject(buffer, false);
                    }
                }
            }
        }
        final int valuesSize = buffer.position();
        collector.recordStats(this, totalSize, updatesSize, valuesSize, maxExceptionsNum);
    }

    @Override
    public String toString() {
        return "BatchCommitUpdatesRequest [" + commitRequests + "]";
    }
}
