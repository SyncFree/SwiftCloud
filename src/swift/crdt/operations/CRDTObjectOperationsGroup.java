package swift.crdt.operations;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTOperation;

/**
 * Representation of a sequence of operations on an object belonging to the same
 * transaction.
 * <p>
 * The sequence of operations shares a base Timestamp (two dimensional
 * timestamp). Each individual operation has a unique TripleTimestamp based on
 * the common timestamp.
 * <p>
 * Thread-safe.
 * 
 * @author mzawirski
 */
public class CRDTObjectOperationsGroup<V extends CRDT<V>> {

    protected CRDTIdentifier id;
    protected CausalityClock dependencyClock;
    protected Timestamp baseTimestamp;
    protected List<CRDTOperation<V>> operations;
    protected V creationState;

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    public CRDTObjectOperationsGroup() {
    }

    /**
     * Constructs a group of operations.
     * 
     * @param id
     * @param dependencyClock
     *            dependency for this group of operations; clock is not copied
     * @param baseTimestamp
     */
    public CRDTObjectOperationsGroup(CRDTIdentifier id, CausalityClock dependencyClock, Timestamp baseTimestamp,
            V creationState) {
        this.id = id;
        this.dependencyClock = dependencyClock;
        this.baseTimestamp = baseTimestamp;
        this.operations = new LinkedList<CRDTOperation<V>>();
        this.creationState = creationState;
    }

    /**
     * @return CRDT identifier for the object on which the operations are
     *         executed
     */
    public CRDTIdentifier getTargetUID() {
        return id;
    }

    /**
     * @return base timestamp of all operations in the sequence
     */
    public Timestamp getBaseTimestamp() {
        return baseTimestamp;
    }

    /**
     * Creates a copy of this group of operations with another base timestamp
     * for all operations in the group.
     * 
     * @param otherBaseTimestamp
     *            base timestamp to be used by all operations in the copy
     * @return a copy of the group with a different base timestamp
     */
    public synchronized CRDTObjectOperationsGroup<V> withBaseTimestamp(Timestamp otherBaseTimestamp) {
        final CRDTObjectOperationsGroup<V> copy = new CRDTObjectOperationsGroup<V>(id, dependencyClock.clone(),
                otherBaseTimestamp, creationState);
        for (final CRDTOperation<V> op : operations) {
            copy.append(op.withBaseTimestamp(otherBaseTimestamp));
        }
        return copy;
    }

    /**
     * Replaces base timestamp of depending operation(s) with the new one for
     * all operations in the group.
     * 
     * @param oldTs
     *            old base timestamp of a dependent operation
     * @param newTs
     *            new base timestamp of a dependent operation
     */
    public synchronized void replaceDependentTimestamp(Timestamp oldTs, Timestamp newTs) {
        dependencyClock.record(newTs);
        // FIXME(mzawirski): CRITICAL remove (or replace) oldTs from the
        // dependencyClock!
        for (CRDTOperation<V> op : operations) {
            op.replaceDependentOpTimestamp(oldTs, newTs);
        }
    }

    /**
     * Returns the minimum causality clock for the object on which the
     * operations are to be executed, representing causal dependencies of this
     * group of operations. Affected by
     * {@link #replaceDependentTimestamp(Timestamp, Timestamp)}.
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
    public synchronized List<CRDTOperation<V>> getOperations() {
        return Collections.unmodifiableList(operations);
    }

    /**
     * Appends a new operation to the sequence of operations.
     * 
     * @param op
     *            next operation to be applied within the transaction
     */
    public synchronized void append(CRDTOperation<V> op) {
        operations.add(op);
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
}
