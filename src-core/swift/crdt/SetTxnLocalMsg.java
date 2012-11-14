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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import swift.application.social.Message;
import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDTQuery;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.operations.SetInsert;
import swift.crdt.operations.SetRemove;

public class SetTxnLocalMsg extends BaseCRDTTxnLocal<SetMsg> {
    private Map<Message, Set<TripleTimestamp>> elems;

    public SetTxnLocalMsg(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, SetMsg creationState,
            Map<Message, Set<TripleTimestamp>> elems) {
        super(id, txn, clock, creationState);
        this.elems = elems;
    }

    /**
     * Insert element e in the set, using the given unique identifier.
     * 
     * @param e
     */
    public void insert(Message e) {
        Set<TripleTimestamp> adds = elems.get(e);
        if (adds == null) {
            adds = new HashSet<TripleTimestamp>();
            elems.put(e, adds);
        }

        TripleTimestamp ts = nextTimestamp();
        adds.add(ts);
        registerLocalOperation(new SetInsert<Message, SetMsg>(ts, e));
    }

    /**
     * Remove element e from the set.
     * 
     * @param e
     */
    public void remove(Message e) {
        Set<TripleTimestamp> ids = elems.remove(e);
        if (ids != null) {
            TripleTimestamp ts = nextTimestamp();
            registerLocalOperation(new SetRemove<Message, SetMsg>(ts, e, ids));
        }
    }

    public boolean lookup(Message e) {
        return elems.containsKey(e);
    }

    public Set<Message> getValue() {
        return elems.keySet();
    }

    @Override
    public Object executeQuery(CRDTQuery<SetMsg> query) {
        return query.executeAt(this);
    }
}
