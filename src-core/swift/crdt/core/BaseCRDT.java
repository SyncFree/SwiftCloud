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
package swift.crdt.core;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;

/**
 * Base class for operation-based CRDT objects.
 * <p>
 * There are two kinds of instances of CRDTs: those associated with transaction
 * handle (with txn handle and clock) and those that are not. The subclasses
 * must provide:
 * <ul>
 * <li>public constructor taking {@link CRDTIdentifier} as a sole argument that
 * is used to create fresh objects by the system (not associated with txn
 * handle),</li>
 * <li>no-argument empty constructor for serialization purposes, and</li>
 * <li>{@link #copy()} method to copy the instance (associated with txn handle
 * or not).</li>
 * <p>
 * Public update methods should register their operations with
 * {@link #registerLocalOperation(CRDTUpdate)} with a custom {@link CRDTUpdate}
 * subclass. Unique TripleTimestamp for update can be generated using
 * {@link #nextTimestamp()} if necessary.
 * 
 * @author mzawirsk, annettebieniusa
 * 
 * @param <V>
 *            target subclass type
 */
public abstract class BaseCRDT<V extends BaseCRDT<V>> implements CRDT<V> {
    // identifier of CRDT from which the object is derived
    protected CRDTIdentifier id;
    // handle to transaction within which the instance is used (can be null)
    protected TxnHandle txn;
    // version of the object (can be null)
    protected CausalityClock clock;

    /**
     * Kryo empty constructor, DO NOT USE for purposes other than serialization.
     */
    public BaseCRDT() {
    }

    /**
     * Creates initial state of CRDT.
     * 
     * @param id
     *            identifier of the CRDT object
     */
    protected BaseCRDT(CRDTIdentifier id) {
        this(id, null, null);
    }

    /**
     * Copying constructor.
     * 
     * @param id
     *            identifier of the CRDT object
     * @param txn
     *            transaction with which the object is registered if this is a
     *            txn-local instance (otherwise null)
     * @param clock
     *            snapshot clock of the object if this is a txn-local instance
     *            (otherwise null)
     */
    protected BaseCRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock) {
        if (id == null) {
            throw new IllegalArgumentException("No id provided for an object");
        }
        this.id = id;
        this.txn = txn;
        this.clock = clock;
    }

    @Override
    public CRDTIdentifier getUID() {
        return this.id;
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

    @Override
    public V copyWith(TxnHandle txnHandle, CausalityClock clock) {
        final V v = copy();
        v.txn = txnHandle;
        v.clock = clock;
        return v;
    }
}
