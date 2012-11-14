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
package swift.crdt.interfaces;

import java.io.Serializable;
import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.clocks.TimestampMapping;
import swift.crdt.BaseCRDT;
import swift.crdt.CRDTIdentifier;
import swift.crdt.operations.CRDTObjectUpdatesGroup;

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
 * @author annettebieniusa, mzawirsk
 * 
 * @param <V>
 *            CvRDT type implementing the interface
 */
public interface CRDT<V extends CRDT<V>> extends Serializable, Copyable {
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
     * In the outcome, updates and clocks of provided object are reflected in
     * this object. Pruning is also unioned, the output pruneClock is merge of
     * the two clocks. The mappings of the instance given in the argument maybe
     * used directly during merge without copying, i.e. the instance in the
     * argument may observe changed mappings after merge.
     * <p>
     * IMPLEMENTATION LIMITATION: note that the incoming crdt pruneClock
     * information may miss local timestamps (the transaction timestamps of
     * scout that is receiving the replica to merge).
     * 
     * @param crdt
     *            object state to merge with
     */
    void merge(CRDT<V> crdt);

    /**
     * Executes a group of atomic operations on this object or updates the
     * timestamp mapping if the operations were already executed.
     * <p>
     * In the outcome, operations and their timestamp are reflected in the state
     * of this object. Note that only a copy of timestamp mappings is used to
     * perform operations, so the original timestamps are well-isolated.
     * 
     * @param ops
     *            operation group to be executed
     * @param dependencyPolicy
     *            policy for dealing with operation group dependencies
     * @return true if operation were executed; false if they were already
     *         included in the state
     * @throws IllegalStateException
     *             when operation's dependencies are not met and checking
     *             dependencies was requested
     */
    boolean execute(CRDTObjectUpdatesGroup<V> ops, CRDTOperationDependencyPolicy dependenciesPolicy);

    /**
     * Prunes the object state to remove versioning meta data from operations
     * dating from before pruningPoint inclusive.
     * <p>
     * After this call returns, snapshots prior or concurrent to pruningPoint
     * will be undefined and should not be requested. Pruning point clock is
     * merged into the clock of an object if checkVersionClock is disabled.
     * 
     * @param pruningPoint
     *            clock up to which data clean-up is performed; without
     *            exceptions
     * @param checkVersionClock
     *            when true, pruningPoint is checked against {@link #getClock()}
     * @throws IllegalStateException
     *             when the provided clock is not greater than or equal to the
     *             existing pruning point
     * @throws IllegalArgumentException
     *             provided clock has disallowed exceptions or checkVersionClock
     *             has been specified and {@link #getClock()} is concurrent or
     *             dominated by pruningPoint
     */
    void prune(CausalityClock pruningPoint, boolean checkVersionClock);

    /**
     * Returns the identifier for the object.
     */
    CRDTIdentifier getUID();

    /**
     * Returns the causality clock including timestamps of all update operations
     * reflected in the object state. The clock may also include timestamps of
     * transactions that did not updated this object.
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
     * Creates a view of an object in particular version provided by the
     * versionClock. Returned view should not share any mutable state with this
     * object, since they can be concurrently modified. If the object is not
     * registered in the store, it should generate a registration operation at
     * this point.
     * 
     * @param versionClock
     *            the returned state is restricted to the specified version
     * @param txn
     * @return a copy of an object, including clocks, uid and txnHandle.
     * @throws IllegalStateException
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
     * Looks for any updates on the crdt since provided clock.
     * 
     * @param clock
     *            clock to look for updates
     * @return set of timestamp mapping copies, representing all known
     *         operations that were not included in the clock; can be empty
     * @throws IllegalArgumentException
     *             when clock dominates or is concurrent to {@link #getClock()},
     *             or clock is dominated or concurrent to
     *             {@link #getPruneClock()}
     */
    Set<TimestampMapping> getUpdatesTimestampMappingsSince(final CausalityClock clock);

    /**
     * @return a deep copy of this object
     */
    V copy();

    /**
     * Auguments update clock of this object with a set of timestamps known to
     * be applied.
     * 
     * @param latestAppliedScoutTimestamp
     *            last timestamp to include in the clock; this timestamp and all
     *            lower timestamp of the same scout will be included
     */
    // FIXME: should be it a CausalityClock, do we allow holes?
    public abstract void augmentWithScoutClock(final Timestamp latestAppliedScoutTimestamp);

    /**
     * Augments update clock of this object with the vector clock of the server,
     * as the missing transactions guaranteedly have not touched the object
     * 
     * @param currentDCClock current DC clock
     */
    public abstract void augmentWithDCClock(final CausalityClock currentDCClock);

    /**
     * Discards from the update clock all timestamps of the provided scout.
     * 
     * @param scoutId
     *            id of the scout whose timestamps will be discarded
     */
    public abstract void discardScoutClock(final String scoutId);

}
