package swift.crdt.operations;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDTOperation;

/**
 * Representation of a sequence of operations on an object belonging to the same
 * transaction.
 * <p>
 * The sequence of operations shares a base Timestamp (two dimensional
 * timestamp). Each individual operations has a unique TripleTimestamp based on
 * the common timestamp.
 * <p>
 * Thread-hostile.
 * 
 * @author mzawirski
 */
public class CRDTObjectOperationsGroup {

    protected CRDTIdentifier id;
    protected CausalityClock dependencyClock;
    protected Timestamp baseTimestamp;
    protected List<CRDTOperation> operations;

    // Fake constructor for Kryo serialization. Do NOT use.
    public CRDTObjectOperationsGroup() {
    }

    public CRDTObjectOperationsGroup(CRDTIdentifier id, CausalityClock dependencyClock, Timestamp baseTimestamp) {
        this.id = id;
        this.dependencyClock = dependencyClock;
        this.baseTimestamp = baseTimestamp;
        this.operations = new LinkedList<CRDTOperation>();
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
     * Replaces old base timestamp with the new base timestamp for all
     * operations in the group.
     * 
     * @param ts
     *            new base timestamp to be used by all operations
     */
    public void replaceBaseTimestamp(Timestamp newBaseTimestamp) {
        baseTimestamp = newBaseTimestamp;
        for (CRDTOperation op : operations) {
            op.replaceBaseTimestamp(newBaseTimestamp);
        }
    }

    /**
     * Returns the causality clock for the object on which the operations are to
     * be executed.
     * 
     * @return causality clock of object state when operations have been issued
     */
    public CausalityClock getDependency() {
        return dependencyClock;
    }

    /**
     * Returns all operations in the group. Note that the returned list a
     * reference to the internal structure and should be retrieved only when all
     * {@link #addOperation(CRDTOperation)} have been performed.
     * 
     * @return all operations on an object, in order they were recorded and
     *         should be applied
     */
    public List<CRDTOperation> getOperations() {
        return Collections.unmodifiableList(operations);
    }

    /**
     * Appends a new operation to the sequence of operations.
     * 
     * @param op
     *            next operation to be applied within the transaction
     */
    public void addOperation(CRDTOperation op) {
        operations.add(op);
    }
}
