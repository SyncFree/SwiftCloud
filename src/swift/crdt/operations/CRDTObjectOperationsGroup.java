package swift.crdt.operations;

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
 * 
 * @author mzawirski
 */
public class CRDTObjectOperationsGroup {

    public CRDTObjectOperationsGroup(CRDTIdentifier id, CausalityClock snapshotClock, Timestamp baseTimestamp) {
        // TODO Auto-generated constructor stub
    }

    /**
     * @return CRDT identifier on which operations are executed
     */
    public CRDTIdentifier getTargetUID() {
        return null;
        // TODO
    }

    /**
     * @return the base timestamp of all operations in the group
     */
    public Timestamp getBaseTimestamp() {
        return null;
        // TODO
    }

    /**
     * Replaces old base timestamp with the new base timestamp for all
     * operations in the group.
     * 
     * @param ts
     *            new base timestamp to use by all operations
     */
    public void replaceBaseTimestamp(Timestamp newBaseTimestamp) {
        // TODO
    }

    /**
     * Returns the causality clock for the objects on which the operation is to
     * be executed.
     * 
     * @return causality clock of object state when operations have been issued.
     */
    public CausalityClock getDependency() {
        return null; // TODO
    }

    /**
     * @return all operations on an object, in order they should be applied 
     */
    public List<CRDTOperation> getOperations() {
        return null; // TODO
    }

    public void addOperation(CRDTOperation op) {
        // TODO Auto-generated method stub
        
    }
}
