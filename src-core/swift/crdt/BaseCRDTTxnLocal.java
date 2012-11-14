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
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTUpdate;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;

/**
 * Base class for implementations of transaction-local views for CRDT objects of
 * the interface {@TxnLocalCRDT}.
 * 
 * This class provides and manages the generic information needed for local
 * queries and updates in the context of a {@TxnHandle}.
 * 
 * @author annettebieniusa, mzawirsk
 * 
 * @param <V>
 *            type implementing the CRDT interface
 */

public abstract class BaseCRDTTxnLocal<V extends CRDT<V>> implements TxnLocalCRDT<V> {
    // handle to transaction within which the object is created
    private final TxnHandle txn;
    // identifier of CRDT from which the object is derived
    protected final CRDTIdentifier id;
    // snapshot clock of the object, reflecting the snapshot
    private final CausalityClock clock;

    /**
     * Constructor for the base representation of transaction-local objects
     * views.
     * 
     * @param id
     *            identifier of the CRDT object from which this txn-local view
     *            is derived
     * @param txn
     *            transaction with which the object is registered
     * @param clock
     *            snapshot clock of the object
     * @param creationState
     *            initial state of the parent if it created within the current
     *            transaction
     */
    public BaseCRDTTxnLocal(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, V creationState) {
        this.txn = txn;
        this.id = id;
        this.clock = clock;
        if (creationState != null) {
            txn.registerObjectCreation(this.id, creationState);
        }
    }

    @Override
    public TxnHandle getTxnHandle() {
        return this.txn;
    }

    /**
     * Gives out timestamps for updates.
     * 
     * @return local timestamp under which the next operation can be registered
     */
    protected TripleTimestamp nextTimestamp() {
        return getTxnHandle().nextTimestamp();
    }

    /**
     * Registers a new update for the CRDT from which the local view is derived
     * as part of the transaction.
     * 
     * @param op
     */
    protected void registerLocalOperation(final CRDTUpdate<V> op) {
        getTxnHandle().registerOperation(this.id, op);
    }

    @Override
    public CausalityClock getClock() {
        return clock;
    }
}
