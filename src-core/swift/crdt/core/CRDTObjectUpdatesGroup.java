/*****************************************************************************
 * Copyright 2011-2014 INRIA
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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.clocks.Timestamp;
import swift.clocks.TimestampMapping;

/**
 * Representation of an atomic sequence of update operations on an object.
 * <p>
 * The sequence of operations shares a base TimestampMapping based on unique
 * client timestamp, which is the unit of visibility of operations group. Each
 * individual operation has a unique id based on the common client timestamp.
 * Additionally, the whole group may have any non-negative number of
 * system-assigned timestamps.
 * <p>
 * Thread-safe.
 * <p>
 * TODO: document life-cycle of mappings and dependencyClock references
 * (optimization hacks in {@link #strippedWithCopiedTimestampMappings()} and
 * {@link #withGlobalDependencyClock(CausalityClock)})
 * 
 * @author mzawirsk
 */
public class CRDTObjectUpdatesGroup<V extends CRDT<V>> {

    protected CRDTIdentifier id;
    protected CausalityClock dependencyClock;
    // the first one is the client timestamp, followed by system timestamp(s)
    protected TimestampMapping timestampMapping;
    protected List<CRDTUpdate<V>> operations;
    protected V creationState;

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    public CRDTObjectUpdatesGroup() {
    }

    /**
     * Constructs a group of operations.
     * 
     * @param id
     * @param timestampMapping
     * @param creationState
     * @param dependencyClock
     */
    public CRDTObjectUpdatesGroup(CRDTIdentifier id, TimestampMapping timestampMapping, V creationState,
            final CausalityClock dependencyClock) {
        this(id, timestampMapping, new LinkedList<CRDTUpdate<V>>(), creationState, dependencyClock);
    }

    private CRDTObjectUpdatesGroup(CRDTIdentifier id, TimestampMapping timestampMapping,
            List<CRDTUpdate<V>> operations, V creationState, final CausalityClock dependencyClock) {
        this.id = id;
        this.timestampMapping = timestampMapping;
        this.operations = operations;
        this.creationState = creationState;
        this.dependencyClock = dependencyClock;
    }

    /**
     * @return CRDT identifier for the object on which the operations are
     *         executed
     */
    public CRDTIdentifier getTargetUID() {
        return id;
    }

    /**
     * @return immutable base client timestamp of all operations in the sequence
     */
    public Timestamp getClientTimestamp() {
        return timestampMapping.getClientTimestamp();
    }

    /**
     * Merge in system timestamp(s) for this transaction from another instance
     * of the updates group. Idempotent.
     * 
     * @param timestampsToMerge
     *            list of timestamps to add
     */
    public synchronized void mergeSystemTimestamps(final CRDTObjectUpdatesGroup<V> group) {
        if (id != null && group.id != null && !id.equals(group.id)) {
            throw new IllegalArgumentException(
                    "Cannot group timestamps for two group of operations on different objects");
        }
        timestampMapping.mergeIn(group.timestampMapping);
    }

    /**
     * Adds a new system timestamp for this transaction. Idempotent.
     * 
     * @param ts
     *            system timestamp to add
     */
    public synchronized void addSystemTimestamp(final Timestamp ts) {
        timestampMapping.addSystemTimestamp(ts);
    }

    /**
     * @return all timestamps currently assigned to this transaction;
     *         unmodifiable view, but not an immutable one
     */
    public synchronized List<Timestamp> getTimestamps() {
        return timestampMapping.getTimestamps();
    }

    /**
     * Checks whether the provided clock includes any timestamp assigned to this
     * updates group.
     * <p>
     * When it returns true, all subsequent calls will also yield true
     * (timestamp mappings can only grow).
     * 
     * @param clock
     *            clock to check against
     * @return true if any timestamp (client or system) used to represent the
     *         transaction of this update intersects with the provided clock
     */
    public synchronized boolean anyTimestampIncluded(final CausalityClock clock) {
        return timestampMapping.anyTimestampIncluded(clock);
    }

    /**
     * Returns the minimum causality clock for the object on which the
     * operations are to be executed, representing causal dependencies of this
     * group of operations.
     * 
     * @return causality clock of object state when operations have been issued
     * 
     */
    public synchronized CausalityClock getDependency() {
        return dependencyClock;
    }

    /**
     * @return read-only reference to the internal list of operations
     *         constituting this group
     */
    public synchronized List<CRDTUpdate<V>> getOperations() {
        return Collections.unmodifiableList(operations);
    }

    /**
     * @return a copy of timestamp mapping used by this group
     */
    public TimestampMapping getTimestampMapping() {
        return timestampMapping.copy();
    }

    /**
     * Appends a new operation to the sequence of operations.
     * 
     * @param op
     *            next operation to be applied within the transaction
     */
    public synchronized void append(CRDTUpdate<V> op) {
        operations.add(op);
    }

    /**
     * Applies all operations in order to the given object instance.
     * 
     * @param crdt
     *            object where operations are applied
     */
    public void applyTo(V crdt) {
        for (final CRDTUpdate<V> u : operations) {
            u.applyTo(crdt);
        }
    }

    /**
     * @return true if this is a create operations containing initial state
     */
    public boolean hasCreationState() {
        return creationState != null;
    }

    /**
     * @return initial state of an object; null if {@link #hasCreationState()}
     *         is false
     */
    public V getCreationState() {
        return creationState;
    }

    /**
     * @param newDependencyClock
     * @param scoutIdToDrop
     * @return shallow copy of this object dependencyClock set to another one.
     */
    public CRDTObjectUpdatesGroup<V> withGlobalDependencyClock(final CausalityClock newDependencyClock,
            String ignoredScoutId) {
        dependencyClock.drop(ignoredScoutId);
        if (newDependencyClock != null
                && !newDependencyClock.compareTo(dependencyClock).is(CMP_CLOCK.CMP_DOMINATES, CMP_CLOCK.CMP_EQUALS)) {
            throw new IllegalArgumentException("new dependency clock is concurrent or lower than the old one");
        }
        return new CRDTObjectUpdatesGroup<V>(id, timestampMapping, operations, creationState, newDependencyClock);
    }

    /**
     * @return shallow copy of this object with a deep copy of timestamps that
     *         may change (the only mutable piece of state at that state), and
     *         stripped out of dependencyClock and object id information
     */
    public CRDTObjectUpdatesGroup<V> strippedWithCopiedTimestampMappings() {
        return new CRDTObjectUpdatesGroup<V>(null, timestampMapping.copy(), operations, creationState, null);
    }

    @Override
    public String toString() {
        return String.format("[id=%s,deps=%s,ts=%s,ops=%s", id, dependencyClock, timestampMapping, operations);
    }
}
