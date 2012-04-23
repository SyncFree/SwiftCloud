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
 * Representation of an atomic sequence of operations on an object.
 * <p>
 * The sequence of operations shares a base Timestamp (two dimensional
 * timestamp), which is the unit of visibility of operations group. Each
 * individual operation has a unique TripleTimestamp based on the common
 * timestamp.
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
     * @param baseTimestamp
     * @param creationState
     */
    public CRDTObjectOperationsGroup(CRDTIdentifier id, Timestamp baseTimestamp, V creationState) {
        this.id = id;
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
     * and dependency clock for all operations in the group.
     * 
     * @param otherBaseTimestamp
     *            base timestamp to be used by all operations in the copy
     * @param dependencyClock
     *            dependency clock for the new copy of operations group
     * @return a copy of the group with a different base timestamp
     */
    public synchronized CRDTObjectOperationsGroup<V> withBaseTimestampAndDependency(Timestamp otherBaseTimestamp,
            final CausalityClock otherDependencyClock) {
        final CRDTObjectOperationsGroup<V> copy = new CRDTObjectOperationsGroup<V>(id, otherBaseTimestamp,
                creationState);
        copy.dependencyClock = otherDependencyClock;
        for (final CRDTOperation<V> op : operations) {
            copy.append(op.withBaseTimestamp(otherBaseTimestamp));
        }
        copy.replaceDependeeOperationTimestamp(baseTimestamp, otherBaseTimestamp);
        return copy;
    }

    /**
     * Replaces base timestamp of dependee operation(s) with the new one for all
     * operations in the group.
     * 
     * @param oldTs
     *            old base timestamp of a dependee operation
     * @param newTs
     *            new base timestamp of a depenedee operation
     */
    public synchronized void replaceDependeeOperationTimestamp(Timestamp oldTs, Timestamp newTs) {
        for (CRDTOperation<V> op : operations) {
            op.replaceDependeeOperationTimestamp(oldTs, newTs);
        }
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
     * Sets the dependency clock of operation group.
     * 
     * @see #getDependency()
     */
    public void setDependency(CausalityClock dependencyClock) {
        this.dependencyClock = dependencyClock;
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
