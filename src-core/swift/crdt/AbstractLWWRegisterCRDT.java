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
package swift.crdt;

import swift.clocks.CausalityClock;
import swift.clocks.IncrementalTripleTimestampGenerator;
import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.core.BaseCRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnGetterSetter;
import swift.crdt.core.TxnHandle;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Abstract implementatio nof aegister CRDT with Last Writer Wins resolution
 * policy for concurrent assignments. WARNING: When assigning to a register,
 * make sure that the elements in the set are either immutable or that they are
 * cloned!
 * 
 * @param <V>
 *            type of the content of a register
 * @author mzawirsk
 */
// TODO: it used to be that Register's content was copyable, but nothing was
// copied actually in the implementation? Current solution is consistent with
// Set CRDTs, but perhaps suboptimal - we could enforce clonability of V and
// clone values just after the API calls.
public abstract class AbstractLWWRegisterCRDT<V, T extends AbstractLWWRegisterCRDT<V, T>> extends BaseCRDT<T> implements
        TxnGetterSetter<V>, KryoSerializable {
    public static TripleTimestamp INIT_TIMESTAMP = new IncrementalTripleTimestampGenerator(new Timestamp("", 1))
            .generateNew();
    protected LWWRegisterUpdate<V, T> lastUpdate;

    // Kryo
    public AbstractLWWRegisterCRDT() {
    }

    public AbstractLWWRegisterCRDT(CRDTIdentifier uid) {
        super(uid);
        lastUpdate = generateUpdate(0, INIT_TIMESTAMP, null);
    }

    protected AbstractLWWRegisterCRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock,
            LWWRegisterUpdate<V, T> lastUpdate) {
        super(id, txn, clock);
        this.lastUpdate = lastUpdate;
    }

    @Override
    public void set(V v) {
        lastUpdate = generateUpdate(++lastUpdate.registerTimestamp, nextTimestamp(), v);
        registerLocalOperation(lastUpdate);
    }

    protected LWWRegisterUpdate<V, T> generateUpdate(long registerTimestamp, TripleTimestamp tiebreakingTimestamp,
            V value) {
        return new LWWRegisterUpdate<V, T>(registerTimestamp, tiebreakingTimestamp, value);
    }

    @Override
    public V getValue() {
        return lastUpdate.val;
    }

    protected void applySet(LWWRegisterUpdate<V, T> update) {
        if (update.compareTo(lastUpdate) > 0) {
            lastUpdate = update;
        }
    }

    @Override
    public void write(Kryo kryo, Output output) {
        baseWrite(kryo, output);
        lastUpdate.write(kryo, output);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        baseRead(kryo, input);
        lastUpdate = generateUpdate(0, null, null);
        lastUpdate.read(kryo, input);
    }
}
