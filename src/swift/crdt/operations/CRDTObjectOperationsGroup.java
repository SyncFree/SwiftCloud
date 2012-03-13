package swift.crdt.operations;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDTOperation;

/**
 * Representation of a sequence of operations on an object that are considered
 * atomic.
 * <p>
 * The sequence of operations share the same state when they were issued, and a
 * base timestamp (two dimensional timestamp). Individual operations may have
 * unique TripleTimestamp based on this timestamp
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
     * @return CRDT identifier on which operations are executed
     */
    public CRDTIdentifier getTargetUID() {
        return id;
    }

    /**
     * @return the base timestamp of all operations in the group
     */
    public Timestamp getBaseTimestamp() {
        return baseTimestamp;
    }

    /**
     * Replaces old base timestamp with the new base timestamp for all
     * operations in the group.
     * 
     * @param ts
     *            new base timestamp to use by all operations
     */
    public void replaceBaseTimestamp(Timestamp newBaseTimestamp) {
        baseTimestamp = newBaseTimestamp;
        for (CRDTOperation op : operations) {
            op.replaceBaseTimestamp(newBaseTimestamp);
        }
    }

    /**
     * Returns the causality clock for the objects on which the operation is to
     * be executed.
     * 
     * @return causality clock of object state when operations have been issued.
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
     * Adds an operation to the sequence.
     * 
     * @param op
     *            operation to add
     */
    public void addOperation(CRDTOperation op) {
        operations.add(op);
    }
}
