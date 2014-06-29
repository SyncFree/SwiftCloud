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

import java.util.HashMap;
import java.util.Map.Entry;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * An optimized version of {@link AbstractPutOnlyLWWMapCRDT} for String type.
 * 
 * @author mzawirsk
 */
public class PutOnlyLWWStringMapCRDT extends AbstractPutOnlyLWWMapCRDT<String, String, PutOnlyLWWStringMapCRDT>
        implements KryoSerializable {
    // Kryo
    public PutOnlyLWWStringMapCRDT() {
    }

    public PutOnlyLWWStringMapCRDT(CRDTIdentifier uid) {
        super(uid);
    }

    private PutOnlyLWWStringMapCRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock,
            HashMap<String, LWWEntry<String>> entries) {
        super(id, txn, clock, entries);
    }

    @Override
    protected PutOnlyLWWStringMapUpdate generatePutDownstream(String key,
            swift.crdt.AbstractPutOnlyLWWMapCRDT.LWWEntry<String> entry) {
        return new PutOnlyLWWStringMapUpdate(key, entry.timestamp, entry.timestampTiebreaker, entry.val);
    }

    @Override
    public PutOnlyLWWStringMapCRDT copy() {
        return new PutOnlyLWWStringMapCRDT(id, txn, clock, new HashMap<String, LWWEntry<String>>(entries));
    }

    @Override
    public void write(Kryo kryo, Output output) {
        baseWrite(kryo, output);
        output.writeVarInt(entries.size(), true);
        for (final Entry<String, LWWEntry<String>> entry : entries.entrySet()) {
            output.writeString(entry.getKey());
            final LWWEntry<String> nestedEntry = entry.getValue();
            output.writeString(nestedEntry.val);
            output.writeVarLong(nestedEntry.timestamp, true);
            nestedEntry.timestampTiebreaker.write(kryo, output);
        }
    }

    @Override
    public void read(Kryo kryo, Input input) {
        baseRead(kryo, input);
        final int entriesSize = input.readVarInt(true);
        this.entries = new HashMap<String, LWWEntry<String>>(entriesSize);
        for (int i = 0; i < entriesSize; i++) {
            final String key = input.readString();
            final LWWEntry<String> entry = new LWWEntry<String>();
            entry.val = input.readString();
            entry.timestamp = input.readVarLong(true);
            entry.timestampTiebreaker = new TripleTimestamp();
            entry.timestampTiebreaker.read(kryo, input);
            entries.put(key, entry);
        }
    }
}
