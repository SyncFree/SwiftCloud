package swift.crdt.interfaces;

import java.io.Serializable;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.BaseCRDT;
import swift.crdt.CRDTIdentifier;
import swift.crdt.operations.CRDTObjectOperationsGroup;

/**
 * Common interface for Commutative Replicated Data Types (CRDTs) definitions.
 * <p>
 * Implementations are encouraged to use {@link BaseCRDT} as a base class.
 * <p>
 * Conceptually, every CRDT object has an associated (1) state made of
 * application of update operations and (2) a clock indicating which updates are
 * reflected in the state. A CRDT object is identified by {@link CRDTIdentifier}
 * , uniquely across the system.
 * <p>
 * Implementation must provide ability of viewing past snapshots of the object
 * (at time specified by {@link #prune(CausalityClock)} or any later time) and
 * generate new update operations. Applications using CRDT object always use it
 * together with transaction handle ({@link TxnHandle}), using a local view
 * obtained by {@link #getTxnLocalCopy(CausalityClock, TxnHandle)}.
 * 
 * @author annettebieniusa
 * 
 * @param <V>
 *            CvRDT type implementing the interface
 */
public interface CRDT<V extends CRDT<V>> extends Serializable {
    // TODO: consider it single-shot method?
    /**
     * Initializes object state. <b>INVOKED ONLY BY SWIFT SYSTEM, ONCE.</b>
     * 
     * @param id
     *            identifier of the object
     * @param clock
     *            causality clock that is associated to the current object
     *            state; object uses this reference directly without copying it
     * @param pruneClock
     *            prune causality clock that is associated to the current object
     *            state; object uses this reference directly without copying it;
     *            pruneClock should be dominated by or equal to clock, which is
     *            NOT verified at init() phase
     * @param registeredInStore
     *            true if object with this identifier has been already
     *            registered in the store; false if the object might not be yet
     *            registered
     * @throws IllegalStateException
     *             if object was already initialized
     */
    void init(CRDTIdentifier id, CausalityClock clock, CausalityClock pruneClock, boolean registeredInStore);

    /**
     * Merges the object with other object state of the same type.
     * <p>
     * In the outcome, updates and clock of provided object are reflected in
     * this object.
     * 
     * TODO: specify pruneClock behavior
     * 
     * @param crdt
     *            object state to merge with
     */
    void merge(CRDT<V> crdt);

    /**
     * Executes a group of atomic operations on this object.
     * <p>
     * In the outcome, operations and their timestamp are reflected in the state
     * of this object.
     * 
     * @param ops
     *            operation group to be executed
     * @param checkDependency
     *            verify that dependencies are included in the clock before
     *            applying the operations
     * @return true if operation were executed; false if they were already
     *         included in the state
     * @throws IllegalStateException
     *             when operation's dependencies are not met and checkDependency
     *             was requested
     */
    boolean execute(CRDTObjectOperationsGroup<V> ops, boolean checkDependency);

    /**
     * Prunes the object state to remove versioning meta data from operations
     * dating from before pruningPoint inclusive.
     * <p>
     * After this call returns, snapshots prior or concurrent to pruningPoint
     * will be undefined and should not be requested. Clock of an object is
     * unaffected.
     * 
     * @param pruningPoint
     *            clock up to which data clean-up is performed; without
     *            exceptions
     * @throws IllegalStateException
     *             when the provided clock is not greater than or equal to the
     *             existing pruning point
     * @throws IllegalArgumentException
     *             provided clock has disallowed exceptions
     */
    void prune(CausalityClock pruningPoint);

    /**
     * Remove the effects of the transaction associated to the timestamp.
     * <p>
     * TODO: what about the clock? TODO Check that rollback is not included in
     * prune clock!
     * 
     * @param ts
     *            time stamp of transaction that is rolled back.
     */
    void rollback(Timestamp ts);

    /**
     * Returns the identifier for the object.
     */
    CRDTIdentifier getUID();

    /**
     * Returns the causality clock including timestamps of all update operations
     * reflected in the object state.
     * 
     * @return causality clock associated to object
     */
    CausalityClock getClock();

    /**
     * Returns the causality clock representing the minimum clock for which
     * versioning of an object is available. Should always be greater or equal
     * {@link #getClock()}.
     * 
     * @return pruned causality clock associated with the object
     */
    CausalityClock getPruneClock();

    /**
     * Creates a copy of an object with optionally restricted state according to
     * versionClock.
     * 
     * @param versionClock
     *            the returned state is restricted to the specified version
     * @param txn
     * @return a copy of an object, including clocks, uid and txnHandle.
     * @throws IllegalArgumentException
     *             when versionClock is not >= {@link #getPruneClock()}
     */
    TxnLocalCRDT<V> getTxnLocalCopy(CausalityClock versionClock, TxnHandle txn);

    /**
     * Returns object registration status in the store.
     * 
     * @return true if object with this identifier has been already registered
     *         in the store; false if the object might not be yet registered.
     */
    boolean isRegisteredInStore();

    /**
     * Marks crdt as registered in store.
     */
    void markRegisteredInStore();

    /**
     * @return a deep copy of this object
     */
    V copy();
}
