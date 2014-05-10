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

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.clocks.ClockFactory;
import swift.clocks.Timestamp;
import swift.clocks.TimestampMapping;

/**
 * Generic manager of an operation-based CRDT implementation V that provides
 * external metadata, versioning, reliable updates execution etc.
 * 
 * The state of the manager is split into three parts:
 * <ol>
 * <li><b>checkpoint</b>: the summary of old updates represented as an
 * operation-based CRDT state (of type V)</li>
 * <li><b>log</b>: log of more recent updates updates represented with a list of
 * (type {@link CRDTUpdate} for CRDT type V)</li>
 * <li><b>metadata</b>: clocks and other information summarizing both parts</li>
 * </ol>
 * 
 * @author mzawirsk,annettebieniusa
 * 
 * @param <V>
 *            type of operation-based CRDT
 */
public class ManagedCRDT<V extends CRDT<V>> {
    // TODO: make costly assertion checks optional.
    private static final long serialVersionUID = 1L;

    private static <V extends CRDT<V>> Map<Timestamp, CRDTObjectUpdatesGroup<V>> getTimestampToUpdatesMap(
            List<CRDTObjectUpdatesGroup<V>> log) {
        final Map<Timestamp, CRDTObjectUpdatesGroup<V>> result = new HashMap<Timestamp, CRDTObjectUpdatesGroup<V>>();
        for (final CRDTObjectUpdatesGroup<V> localUpdate : log) {
            result.put(localUpdate.getClientTimestamp(), localUpdate);
        }
        return result;
    }

    // WISHME: we can make some of these fields transient (e.g., id and
    // registeredInStore) for sake of optimized storage.

    // id under which the CRDT is globally known and uniquely identified
    protected CRDTIdentifier id;
    // clock with the current local state of this CRDT, comprises all timestamps
    // of updates performed on this replica
    protected CausalityClock clock;
    // clock of the checkpoint state only
    protected CausalityClock pruneClock;
    // registration status
    protected boolean registeredInStore;
    protected V checkpoint;
    // log of updates, in some linear extension of causality, stripped of
    // unnecessary information (dependency clocks and ids)
    protected List<CRDTObjectUpdatesGroup<V>> strippedLog;

    public ManagedCRDT() {
    }

    /**
     * Created instance with the provided initial checkpoint and metadata.
     * 
     * @param id
     *            identifier of the object
     * @param initialCheckpoint
     *            initial state for the first checkpoint
     * @param clock
     *            causality clock that is associated to the current object
     *            state; object uses this reference directly without copying it
     * @param registeredInStore
     *            true if object with this identifier has been already
     *            registered in the store; false if the object might not be yet
     *            registered
     * @throws IllegalStateException
     *             if object was already initialized
     */
    public ManagedCRDT(CRDTIdentifier id, V initialCheckpoint, CausalityClock clock, boolean registeredInStore) {
        this.id = id;
        this.checkpoint = initialCheckpoint;
        this.strippedLog = new LinkedList<CRDTObjectUpdatesGroup<V>>();
        this.clock = clock;
        this.pruneClock = ClockFactory.newClock();
        this.registeredInStore = registeredInStore;
    }

    /**
     * Returns the identifier for the object.
     */
    public CRDTIdentifier getUID() {
        return this.id;
    }

    /**
     * Returns object registration status in the store.
     * 
     * @return true if object with this identifier has been already registered
     *         in the store; false if the object might not be yet registered.
     *         once registered it will remain so
     */
    public boolean isRegisteredInStore() {
        return registeredInStore;
    }

    /**
     * Returns the causality clock including timestamps of all update operations
     * reflected in the object state. The clock may also include timestamps of
     * transactions that did not updated this object.
     * 
     * @return causality clock associated to object
     */
    public CausalityClock getClock() {
        return clock;
    }

    /**
     * Returns the causality clock representing the minimum clock for which
     * versioning of an object is available. Smaller than or equal to
     * {@link #getClock()}.
     * 
     * @return pruned causality clock associated with the object
     */
    public CausalityClock getPruneClock() {
        return pruneClock;
    }

    /**
     * Augments update clock of this object with the timestamp of some scout
     * assuming this object includes all the updates with that timestamp and
     * knows all the mappings for them.
     * 
     * @param latestAppliedScoutTimestamp
     */
    public void augmentWithScoutTimestamp(Timestamp clientTimestamp) {
        clock.record(clientTimestamp);
    }

    /**
     * Augments update clock of this object with the clock of some scout
     * assuming this object includes all the transactions generated by that
     * scout with lower or equal timestamp, and knows all the mappings for them.
     * 
     * @param latestAppliedScoutTimestamp
     */
    public void augmentWithScoutClockWithoutMappings(final Timestamp latestAppliedScoutTimestamp) {
        clock.recordAllUntil(latestAppliedScoutTimestamp);
        // WISHME: ideally, we could have information on
        // latestAppliedScoutPRUNEDTimestamp and apply it to pruneClock rather
        // than clock, but DC provides information at the level of all updates,
        // not pruned ones, so we deal with it.
    }

    /**
     * Discards from the update clock all timestamps of the provided scout.
     * 
     * @param scoutId
     *            id of the scout whose timestamps will be discarded
     */
    public void discardScoutClock(final String scoutId) {
        clock.drop(scoutId);
    }

    /**
     * Augments update clock of this object with the vector clock of the server,
     * assuming this object includes all the transactions represented by this
     * clock, and knows all the mappings for them.
     * 
     * @param currentDCClock
     *            current DC clock
     */
    public void augmentWithDCClockWithoutMappings(final CausalityClock currentDCClock) {
        clock.merge(currentDCClock);
    }

    /**
     * Prunes the object state to remove versioning meta data from operations
     * dating from before pruningPoint inclusive.
     * <p>
     * After this call returns, snapshots prior or concurrent to pruningPoint
     * will be undefined and should not be requested. Pruning point clock is
     * merged into the clock of an object if checkVersionClock is disabled.
     * 
     * @param pruningPoint
     *            clock that represents updates to clean-up for versioning
     *            purposes (merged with existing pruningPoint)
     * @param checkVersionClock
     *            when true, pruningPoint is checked against {@link #getClock()}
     * @throws IllegalArgumentException
     *             provided checkVersionClock has been specified and
     *             {@link #getClock()} is concurrent or dominated by
     *             pruningPoint
     */
    public void prune(CausalityClock pruningPoint, boolean checkVersionClock) {
        if (checkVersionClock && clock.compareTo(pruningPoint).is(CMP_CLOCK.CMP_CONCURRENT, CMP_CLOCK.CMP_ISDOMINATED)) {
            throw new IllegalStateException("Cannot prune concurrently or later than updates clock of this version");
        }

        clock.merge(pruningPoint);
        final CMP_CLOCK cmpPrune = pruneClock.merge(pruningPoint);
        if (cmpPrune.is(CMP_CLOCK.CMP_CONCURRENT, CMP_CLOCK.CMP_ISDOMINATED)) {
            for (final Iterator<CRDTObjectUpdatesGroup<V>> updatesIter = strippedLog.iterator(); updatesIter.hasNext();) {
                final CRDTObjectUpdatesGroup<V> updates = updatesIter.next();
                if (updates.anyTimestampIncluded(pruneClock)) {
                    updates.applyTo(checkpoint);
                    updatesIter.remove();
                }
            }
        }
    }

    /**
     * Merges the state of this managed object with other managed object of the
     * same identity and type.
     * <p>
     * In the outcome, updates and timestamps of provided object are reflected
     * in this object. Merge is a best-effort logic that may fail with
     * {@link IllegalStateException}, but it is not expected given SwiftCloud's
     * invariants.
     * <p>
     * IMPLEMENTATION ASSUMPTION: the incoming crdt pruneClock may miss scout's
     * local timestamps, but the clock should not miss them.
     * 
     * @param crdt
     *            object state to merge with; unmodified
     * @throws IllegalStateException
     *             if merge heuristic fails to merge the two objects
     */
    public void merge(ManagedCRDT<V> other) {
        if (!id.equals(other.id)) {
            throw new IllegalArgumentException("Refusing to merge two objects with different identities: " + id
                    + " vs " + other.id);
        }

        // This is a somewhat messy best-effort logic, since merge is not
        // exactly symmetric for op-based.

        final Map<Timestamp, CRDTObjectUpdatesGroup<V>> thisTimestampToUpdatesMap = getTimestampToUpdatesMap(strippedLog);
        switch (getClock().compareTo(other.getClock())) {
        case CMP_DOMINATES:
        case CMP_EQUALS:
            // Easy case, not much to do.
            // Heuristic: resolve a potential difference in pruning point in
            // favor of "this" over "other".
            // Merge timestamp mappings. (not sure if this is strictly
            // necessary, but let's do it)
            for (final CRDTObjectUpdatesGroup<V> otherUpdate : other.strippedLog) {
                final CRDTObjectUpdatesGroup<V> localMatch = thisTimestampToUpdatesMap.get(otherUpdate
                        .getClientTimestamp());
                if (localMatch != null) {
                    localMatch.mergeSystemTimestamps(otherUpdate);
                }
            }
            break;
        case CMP_ISDOMINATED:
            // The exact opposite of the above case.
            this.checkpoint = other.checkpoint.copy();
            this.pruneClock = other.pruneClock.clone();
            this.clock = other.clock.clone();
            final List<CRDTObjectUpdatesGroup<V>> newLog = new LinkedList<CRDTObjectUpdatesGroup<V>>();
            for (final CRDTObjectUpdatesGroup<V> otherUpdate : other.strippedLog) {
                final CRDTObjectUpdatesGroup<V> copiedOtherUpdate = otherUpdate.strippedWithCopiedTimestampMappings();
                newLog.add(copiedOtherUpdate);
                final CRDTObjectUpdatesGroup<V> localMatch = thisTimestampToUpdatesMap.get(otherUpdate
                        .getClientTimestamp());
                if (localMatch != null) {
                    copiedOtherUpdate.mergeSystemTimestamps(localMatch);
                }
            }
            this.strippedLog = newLog;
            break;
        case CMP_CONCURRENT:
            // The most tricky case. The following logic is a heuristic that
            // targets a scout that merges in a version from a DC.
            // It may not cover other cases, but we do not expect them in
            // or can fall back to refetching object from scratch.
            // The heuristic relies on an invariant that if some update was
            // included in A.checkpoint and B.clock >= A.clock, then the update
            // must be present in either B.checkpoint or B.log
            if (!pruneClock.compareTo(other.clock).is(CMP_CLOCK.CMP_ISDOMINATED, CMP_CLOCK.CMP_EQUALS)) {
                throw new IllegalStateException(
                        "Unsupported attempt to merge objects with concurrent clocks and a clock concurrent to pruning point");
            }
            this.checkpoint = other.checkpoint;
            this.pruneClock = other.pruneClock.clone();
            this.clock.merge(other.clock);
            final List<CRDTObjectUpdatesGroup<V>> mergedLog = new LinkedList<CRDTObjectUpdatesGroup<V>>();
            // Copy other's log and merge timestamps with all local updates.
            for (final CRDTObjectUpdatesGroup<V> otherUpdate : other.strippedLog) {
                final CRDTObjectUpdatesGroup<V> copiedOtherUpdate = otherUpdate.strippedWithCopiedTimestampMappings();
                mergedLog.add(copiedOtherUpdate);
                final CRDTObjectUpdatesGroup<V> localMatch = thisTimestampToUpdatesMap.get(otherUpdate
                        .getClientTimestamp());
                if (localMatch != null) {
                    copiedOtherUpdate.mergeSystemTimestamps(localMatch);
                }
            }
            final Map<Timestamp, CRDTObjectUpdatesGroup<V>> mergedTimestampToUpdatesMap = getTimestampToUpdatesMap(mergedLog);
            // Apply this log entries that are not in other's checkpoint/log.
            for (final CRDTObjectUpdatesGroup<V> thisUpdate : strippedLog) {
                if (thisUpdate.anyTimestampIncluded(other.clock)) {
                    final CRDTObjectUpdatesGroup<V> mergedUpdate = mergedTimestampToUpdatesMap.get(thisUpdate
                            .getClientTimestamp());
                    if (mergedUpdate != null) {
                        mergedUpdate.mergeSystemTimestamps(thisUpdate);
                    }
                    // else: skip - it may be in the checkpoint, while
                    // pruneClock does not contain scout's entry. See note
                    // in #augmentWithScoutClock
                } else {
                    if (mergedTimestampToUpdatesMap.containsKey(thisUpdate.getClientTimestamp())) {
                        throw new IllegalArgumentException(
                                "The incoming CRDT contains updates that are not included in its clock");
                    }
                    mergedLog.add(thisUpdate.strippedWithCopiedTimestampMappings());
                }
            }

            this.strippedLog = mergedLog;
        }
        registeredInStore |= other.registeredInStore;
    }

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
    public boolean execute(CRDTObjectUpdatesGroup<V> ops, final CRDTOperationDependencyPolicy dependenciesPolicy) {
        final CausalityClock dependencyClock = ops.getDependency();
        if (dependenciesPolicy == CRDTOperationDependencyPolicy.CHECK) {
            final CMP_CLOCK dependencyCmp = clock.compareTo(dependencyClock);
            if (dependencyCmp == CMP_CLOCK.CMP_ISDOMINATED || dependencyCmp == CMP_CLOCK.CMP_CONCURRENT) {
                throw new IllegalStateException("Object does not meet operation's dependencies");
            }
        } else if (dependenciesPolicy == CRDTOperationDependencyPolicy.RECORD_BLINDLY) {
            clock.merge(dependencyClock);
        } // else: dependenciesPolicy ==IGNORE

        if (ops.hasCreationState()) {
            registeredInStore = true;
        }

        boolean newOperation = true;
        for (final Timestamp timestamp : ops.getTimestamps()) {
            newOperation &= clock.record(timestamp);
        }
        if (newOperation) {
            strippedLog.add(ops.strippedWithCopiedTimestampMappings());
        } else if (!ops.anyTimestampIncluded(pruneClock)) {
            for (final CRDTObjectUpdatesGroup<V> existingOps : strippedLog) {
                if (existingOps.getClientTimestamp().equals(ops.getClientTimestamp())) {
                    existingOps.mergeSystemTimestamps(existingOps);
                    break;
                }
            }
        }

        return newOperation;
    }

    /**
     * Creates a version of the object for the provided clock. Returned view
     * does not share any mutable state with this object, since they can be
     * concurrently modified. If the object is not registered in the store, it
     * generates a registration operation at this point.
     * 
     * @param versionClock
     *            the returned state is restricted to the specified version
     * @param txn
     *            transaction handle that will be associated with the returned
     *            version
     * @return a copy of an object with the provided clocks and txnHandle.
     * @throws IllegalStateException
     *             if it is not the case that {@link #getPruneClock()} <=
     *             versionClock <= {@link #getClock()}
     */
    // WISHME: we could have returned a slightly stripped view to the client
    // wish only observable content + necessary metadata, but in practice
    // that's often almost the same (OR-Set) or just the same (e.g. Counter)
    public V getVersion(CausalityClock versionClock, TxnHandle txn) {
        assertGreaterEqualsPruneClock(versionClock);
        assertLessEqualsClock(versionClock);

        if (!isRegisteredInStore()) {
            // It is safe to register it once, since a TxnHandle instance
            // calls getVersion exactly once.
            txn.registerObjectCreation(id, (V) checkpoint.copy());
        }

        final V version = (V) checkpoint.copyWith(txn, versionClock.clone());
        for (final CRDTObjectUpdatesGroup<V> updates : strippedLog) {
            if (updates.anyTimestampIncluded(versionClock)) {
                updates.applyTo(version);
            }
        }
        return version;
    }

    public V getLatestVersion(TxnHandle txn) {
        // WISHME: cache the most recent version? it should be easy.
        return getVersion(getClock(), txn);
    }

    protected void assertLessEqualsClock(CausalityClock clock) {
        if (getClock() == null) {
            return;
        }
        final CMP_CLOCK clockCmp = getClock().compareTo(clock);
        if (clockCmp == CMP_CLOCK.CMP_CONCURRENT || clockCmp == CMP_CLOCK.CMP_ISDOMINATED) {
            throw new IllegalStateException("provided clock (" + clock
                    + ") is not less or equal to the object updates clock (" + getClock() + ")");
        }
    }

    protected void assertGreaterEqualsPruneClock(CausalityClock clock) {
        if (getPruneClock() == null) {
            return;
        }
        final CMP_CLOCK clockCmp = getPruneClock().compareTo(clock);
        if (clockCmp == CMP_CLOCK.CMP_CONCURRENT || clockCmp == CMP_CLOCK.CMP_DOMINATES) {
            throw new IllegalStateException("provided clock (" + clock
                    + ") is not greater or equal to the prune clock (" + getPruneClock() + ")");
        }
    }

    /**
     * Looks for any updates on the crdt since provided clock.
     * 
     * @param lowerBoundClock
     *            clock to look for updates
     * @return set of timestamp mapping copies, representing all known
     *         operations that were not included in the clock; can be empty
     * @throws IllegalArgumentException
     *             when clock dominates or is concurrent to {@link #getClock()},
     *             or clock is dominated or concurrent to
     *             {@link #getPruneClock()}
     */
    public List<TimestampMapping> getUpdatesTimestampMappingsSince(CausalityClock lowerBoundClock) {
        if (lowerBoundClock.compareTo(pruneClock).is(CMP_CLOCK.CMP_CONCURRENT, CMP_CLOCK.CMP_ISDOMINATED)) {
            throw new IllegalArgumentException();
        }

        final List<TimestampMapping> result = new LinkedList<TimestampMapping>();
        for (final CRDTObjectUpdatesGroup<V> updates : strippedLog) {
            if (!updates.anyTimestampIncluded(lowerBoundClock)) {
                result.add(updates.getTimestampMapping());
            }
        }
        return result;
    }

    /**
     * @param versioningLowerBound
     *            events contained in this clock (restricted to object's
     *            version) won't be available for versioning
     * @return a deep copy of the object for replication purposes, with
     *         restricted versioning
     */
    public ManagedCRDT<V> copyWithRestrictedVersioning(final CausalityClock versioningLowerBound) {
        final ManagedCRDT<V> result = new ManagedCRDT<V>();
        result.id = id;
        result.clock = clock.clone();

        result.pruneClock = versioningLowerBound.clone();
        result.pruneClock.intersect(result.clock);
        result.pruneClock.merge(pruneClock);
        result.registeredInStore = registeredInStore;
        result.checkpoint = checkpoint.copy();
        result.strippedLog = new LinkedList<CRDTObjectUpdatesGroup<V>>();
        for (final CRDTObjectUpdatesGroup<V> updates : strippedLog) {
            if (updates.anyTimestampIncluded(result.pruneClock)) {
                updates.applyTo(result.checkpoint);
            } else {
                result.strippedLog.add(updates.strippedWithCopiedTimestampMappings());
            }
        }
        return result;
    }

    @Override
    public String toString() {
        return String.format("[id=%s,clock=%s,pruneClock=%s,registered=%b,checkpoint=%s,log=%s", id, clock, pruneClock,
                registeredInStore, checkpoint, strippedLog);
    }
}
