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
package swift.crdt;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDTUpdate;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;

public class IntegerVersioned extends BaseCRDT<IntegerVersioned> {

    private static final long serialVersionUID = 1L;
    // Map of updateId to integer deltas
    private Map<TripleTimestamp, Integer> updates;
    // Current value with respect to the updatesClock
    private int currentValue;

    // Value with respect to the pruneClock
    private int pruneValue;
    private Map<String, Integer> prunedValuesPerSite;

    public IntegerVersioned() {
        this.updates = new HashMap<TripleTimestamp, Integer>();
        this.prunedValuesPerSite = new HashMap<String, Integer>();
    }

    protected int value(CausalityClock snapshotClock) {
        if (snapshotClock.compareTo(getClock()) != CMP_CLOCK.CMP_ISDOMINATED) {
            // Since snapshot covers all updates making up this object, use the
            // current value.
            return currentValue;
        }
        int retValue = pruneValue;
        retValue += getAggregateOfUpdatesIncluded(snapshotClock);
        return retValue;
    }

    /**
     * @param clk
     *            clock restricting the set of updates; null represents a clock
     *            including all updates
     * @return aggregate value of updates included in clk
     */
    private int getAggregateOfUpdatesIncluded(CausalityClock clk) {
        int acc = 0;

        for (final Entry<TripleTimestamp, Integer> update : updates.entrySet()) {
            if (clk == null || update.getKey().timestampsIntersect(clk)) {
                acc += update.getValue();
            }
        }
        return acc;
    }

    public void applyUpdate(int n, TripleTimestamp ts) {
        updates.put(ts, n);
        registerTimestampUsage(ts);
        currentValue += n;
    }

    @Override
    protected boolean mergePayload(IntegerVersioned other) {
        mergePrunedPayload(other);

        // Look for individual local updates that we can now remove because we
        // merged them in as a pruned state.
        final Iterator<Entry<TripleTimestamp, Integer>> thisUpdatesIter = updates.entrySet().iterator();
        while (thisUpdatesIter.hasNext()) {
            final Entry<TripleTimestamp, Integer> update = thisUpdatesIter.next();
            final TripleTimestamp uid = update.getKey();
            // TODO: this is a really trick code here, due to the fact we
            // other.getPruneClock() may be imprecise - it misses local
            // timestamps.
            // It means there may exist an update in this.updates that is using
            // a client timestamp mapping only, which is included in other.clock
            // and actually pruned in other's payload, but the local timestamp
            // is not included in other.pruneClock. Yuck!
            if (uid.timestampsIntersect(other.getClock())) {
                if (!other.updates.containsKey(uid)) {
                    thisUpdatesIter.remove();
                    unregisterTimestampUsage(uid);
                }
            }
        }

        // Look for individual remote updates that we should incorporate.
        final Iterator<Entry<TripleTimestamp, Integer>> otherUpdatesIter = other.updates.entrySet().iterator();
        while (otherUpdatesIter.hasNext()) {
            final Entry<TripleTimestamp, Integer> update = otherUpdatesIter.next();
            final TripleTimestamp uid = update.getKey();
            if (!uid.timestampsIntersect(getClock())) {
                updates.put(uid, update.getValue());
                registerTimestampUsage(uid);
            }
        }

        // Update current value.
        currentValue = pruneValue + getAggregateOfUpdatesIncluded(null);
        return false;
    }

    private void mergePrunedPayload(IntegerVersioned other) {
        int sumOfNewlyPrunedUpdates = 0;
        // Update more recent entries.
        for (final Entry<String, Integer> otherPrunedValue : other.prunedValuesPerSite.entrySet()) {
            final String site = otherPrunedValue.getKey();
            if (other.getPruneClock().getLatestCounter(site) > getPruneClock().getLatestCounter(site)) {
                Integer oldValue = prunedValuesPerSite.put(site, otherPrunedValue.getValue());
                if (oldValue == null) {
                    oldValue = 0;
                }
                sumOfNewlyPrunedUpdates += otherPrunedValue.getValue() - oldValue;
            }
        }
        // Discard 0-ed more recent entries
        final Iterator<Entry<String, Integer>> localPrunedValueIter = prunedValuesPerSite.entrySet().iterator();
        while (localPrunedValueIter.hasNext()) {
            final Entry<String, Integer> entry = localPrunedValueIter.next();
            final String site = entry.getKey();
            if (other.getPruneClock().getLatestCounter(site) > getPruneClock().getLatestCounter(site)
                    && !other.prunedValuesPerSite.containsKey(site)) {
                sumOfNewlyPrunedUpdates -= entry.getValue();
            }
        }
        pruneValue += sumOfNewlyPrunedUpdates;
    }

    @Override
    protected void pruneImpl(CausalityClock c) {
        int sumOfNewlyPrunedUpdates = 0;
        Iterator<Entry<TripleTimestamp, Integer>> updatesIter = updates.entrySet().iterator();
        while (updatesIter.hasNext()) {
            final Entry<TripleTimestamp, Integer> update = updatesIter.next();
            final TripleTimestamp uid = update.getKey();
            if (uid.timestampsIntersect(c)) {
                final String summarySite = uid.getSelectedSystemTimestamp().getIdentifier();
                Integer value = prunedValuesPerSite.get(summarySite);
                if (value == null) {
                    value = 0;
                }
                value += update.getValue();
                sumOfNewlyPrunedUpdates += update.getValue();
                if (value != 0) {
                    prunedValuesPerSite.put(summarySite, value);
                } else {
                    prunedValuesPerSite.remove(summarySite);
                }
                updatesIter.remove();
            }
        }
        pruneValue += sumOfNewlyPrunedUpdates;
    }

    protected TxnLocalCRDT<IntegerVersioned> getTxnLocalCopyImpl(CausalityClock versionClock, TxnHandle txn) {
        final IntegerVersioned creationState = isRegisteredInStore() ? null : new IntegerVersioned();
        IntegerTxnLocal localView = new IntegerTxnLocal(id, txn, versionClock, creationState, value(versionClock));
        return localView;
    }

    @Override
    protected void execute(CRDTUpdate<IntegerVersioned> op) {
        op.applyTo(this);
    }

    @Override
    public IntegerVersioned copy() {
        IntegerVersioned copy = new IntegerVersioned();
        for (final Entry<TripleTimestamp, Integer> update : updates.entrySet()) {
            copy.updates.put(update.getKey(), update.getValue());
        }
        copy.currentValue = this.currentValue;
        copy.prunedValuesPerSite.putAll(this.prunedValuesPerSite);
        copy.pruneValue = this.pruneValue;
        copyBase(copy);
        return copy;
    }
}
