package swift.crdt.payload;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;

/**
 * Helper class with static methods for merging common types of payload. The
 * methods are used to reduce code duplication and simplify the maintenance.
 * 
 * @author annettebieniusa, mzawirsk
 * 
 */
public final class PayloadHelper {

    public static <V extends Object> void mergePayload(Map<V, Map<TripleTimestamp, Set<TripleTimestamp>>> base,
            CausalityClock baseClock, CausalityClock basePrunePoint,
            Map<V, Map<TripleTimestamp, Set<TripleTimestamp>>> other, CausalityClock otherClock,
            CausalityClock otherPrunePoint) {
        Iterator<Entry<V, Map<TripleTimestamp, Set<TripleTimestamp>>>> it = other.entrySet().iterator();
        while (it.hasNext()) {
            Entry<V, Map<TripleTimestamp, Set<TripleTimestamp>>> e = it.next();

            Map<TripleTimestamp, Set<TripleTimestamp>> s = base.get(e.getKey());
            if (s == null) {
                Map<TripleTimestamp, Set<TripleTimestamp>> newSet = new HashMap<TripleTimestamp, Set<TripleTimestamp>>(
                        e.getValue());
                base.put(e.getKey(), newSet);

            } else {
                for (Entry<TripleTimestamp, Set<TripleTimestamp>> otherE : e.getValue().entrySet()) {
                    boolean exists = false;
                    for (Entry<TripleTimestamp, Set<TripleTimestamp>> localE : s.entrySet()) {
                        if (localE.getKey().equals(otherE.getKey())) {
                            localE.getValue().addAll(otherE.getValue());
                            exists = true;
                        }
                    }
                    if (!exists && !baseClock.includes(otherE.getKey())) {
                        s.put(otherE.getKey(), new HashSet<TripleTimestamp>(otherE.getValue()));
                    }
                    // else if !exists: otherE has been locally removed&pruned,
                    // do not add it again
                }
            }
        }

        // Remove elements removed&pruned at the incoming replica.
        Iterator<Entry<V, Map<TripleTimestamp, Set<TripleTimestamp>>>> ourIt = base.entrySet().iterator();
        while (ourIt.hasNext()) {
            Entry<V, Map<TripleTimestamp, Set<TripleTimestamp>>> ourE = ourIt.next();
            Map<TripleTimestamp, Set<TripleTimestamp>> otherInstances = other.get(ourE.getKey());
            if (otherInstances == null) {
                // Consider dummy empty map in this case.
                otherInstances = new HashMap<TripleTimestamp, Set<TripleTimestamp>>();
            }
            final Iterator<Entry<TripleTimestamp, Set<TripleTimestamp>>> ourInstanceIter = ourE.getValue().entrySet()
                    .iterator();
            while (ourInstanceIter.hasNext()) {
                final Entry<TripleTimestamp, Set<TripleTimestamp>> ourInstance = ourInstanceIter.next();
                if (otherClock.includes(ourInstance.getKey()) && !otherInstances.containsKey(ourInstance.getKey())) {
                    // ourInstance has been removed and pruned at the remote
                    // replica.
                    ourInstanceIter.remove();
                }
            }
        }
    }

    public static <V extends Object> Set<Timestamp> getUpdateTimestampsSinceImpl(
            Map<V, Map<TripleTimestamp, Set<TripleTimestamp>>> base, CausalityClock clock) {
        final Set<Timestamp> result = new HashSet<Timestamp>();
        for (Map<TripleTimestamp, Set<TripleTimestamp>> addsRemoves : base.values()) {
            for (final Entry<TripleTimestamp, Set<TripleTimestamp>> addRemoves : addsRemoves.entrySet()) {
                if (!clock.includes(addRemoves.getKey())) {
                    result.add(addRemoves.getKey().cloneBaseTimestamp());
                }
                for (final TripleTimestamp removeTimestamp : addRemoves.getValue()) {
                    if (!clock.includes(removeTimestamp)) {
                        result.add(removeTimestamp.cloneBaseTimestamp());
                    }
                }
            }
        }
        return result;
    }

    public static <V extends Object> void rollback(Map<V, Map<TripleTimestamp, Set<TripleTimestamp>>> base,
            Timestamp rollbackEvent) {
        Iterator<Entry<V, Map<TripleTimestamp, Set<TripleTimestamp>>>> entries = base.entrySet().iterator();
        while (entries.hasNext()) {
            Entry<V, Map<TripleTimestamp, Set<TripleTimestamp>>> e = entries.next();
            Iterator<Map.Entry<TripleTimestamp, Set<TripleTimestamp>>> perClient = e.getValue().entrySet().iterator();
            while (perClient.hasNext()) {
                Entry<TripleTimestamp, Set<TripleTimestamp>> valueTS = perClient.next();
                if (valueTS.getKey().equals(rollbackEvent)) {
                    perClient.remove();
                } else {
                    Iterator<TripleTimestamp> remTS = valueTS.getValue().iterator();
                    while (remTS.hasNext()) {
                        if (remTS.next().equals(rollbackEvent)) {
                            remTS.remove();
                        }
                    }
                }
            }
            if (e.getValue().isEmpty()) {
                entries.remove();
            }
        }
    }

    public static <V extends Object> void pruneImpl(Map<V, Map<TripleTimestamp, Set<TripleTimestamp>>> base,
            CausalityClock pruningPoint) {
        Iterator<Entry<V, Map<TripleTimestamp, Set<TripleTimestamp>>>> entries = base.entrySet().iterator();
        while (entries.hasNext()) {
            Entry<V, Map<TripleTimestamp, Set<TripleTimestamp>>> e = entries.next();
            Iterator<Map.Entry<TripleTimestamp, Set<TripleTimestamp>>> perClient = e.getValue().entrySet().iterator();
            while (perClient.hasNext()) {
                Map.Entry<TripleTimestamp, Set<TripleTimestamp>> current = perClient.next();
                Iterator<TripleTimestamp> removals = current.getValue().iterator();
                while (removals.hasNext()) {
                    TripleTimestamp ts = removals.next();
                    if (pruningPoint.includes(ts)) {
                        perClient.remove();
                        break;
                    }
                }
            }
            if (e.getValue().isEmpty()) {
                entries.remove();
            }
        }
    }

    public static <V extends Object> Map<V, Set<TripleTimestamp>> getValue(
            Map<V, Map<TripleTimestamp, Set<TripleTimestamp>>> base, CausalityClock snapshotClock) {
        Map<V, Set<TripleTimestamp>> retValues = new HashMap<V, Set<TripleTimestamp>>();
        Set<Entry<V, Map<TripleTimestamp, Set<TripleTimestamp>>>> entrySet = base.entrySet();
        for (Entry<V, Map<TripleTimestamp, Set<TripleTimestamp>>> e : entrySet) {
            Set<TripleTimestamp> present = new HashSet<TripleTimestamp>();
            for (Entry<TripleTimestamp, Set<TripleTimestamp>> p : e.getValue().entrySet()) {
                if (snapshotClock.includes(p.getKey())) {
                    boolean add = true;
                    for (TripleTimestamp remTs : p.getValue()) {
                        if (snapshotClock.includes(remTs)) {
                            add = false;
                            break;
                        }
                    }
                    if (add) {
                        present.add(p.getKey());
                    }
                }
            }

            if (!present.isEmpty()) {
                retValues.put(e.getKey(), present);
            }
        }
        return retValues;
    }

}
