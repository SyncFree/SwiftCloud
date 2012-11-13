package swift.crdt;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;

/**
 * Helper class with static methods for merging common types of payload. The
 * methods are used to reduce code duplication and simplify the maintenance.
 * 
 * @author annettebieniusa, mzawirsk
 * 
 */
public final class AddWinsUtils {

    public static <V extends Object> void mergePayload(Map<V, Map<TripleTimestamp, Set<TripleTimestamp>>> base,
            CausalityClock baseClock, Map<V, Map<TripleTimestamp, Set<TripleTimestamp>>> other,
            CausalityClock otherClock, List<TripleTimestamp> outputTimestampsToRegister,
            List<TripleTimestamp> outputTimestampsToUnregister) {
        Iterator<Entry<V, Map<TripleTimestamp, Set<TripleTimestamp>>>> it = other.entrySet().iterator();
        while (it.hasNext()) {
            Entry<V, Map<TripleTimestamp, Set<TripleTimestamp>>> e = it.next();

            Map<TripleTimestamp, Set<TripleTimestamp>> s = base.get(e.getKey());
            if (s == null) {
                s = new HashMap<TripleTimestamp, Set<TripleTimestamp>>();
                base.put(e.getKey(), s);
            }
            for (Entry<TripleTimestamp, Set<TripleTimestamp>> otherE : e.getValue().entrySet()) {
                boolean exists = false;
                for (Entry<TripleTimestamp, Set<TripleTimestamp>> localE : s.entrySet()) {
                    if (localE.getKey().equals(otherE.getKey())) {
                        for (final TripleTimestamp otherRemoveTs : otherE.getValue()) {
                            if (localE.getValue().add(otherRemoveTs)) {
                                outputTimestampsToRegister.add(otherRemoveTs);
                            }
                        }
                        exists = true;
                    }
                }
                if (!exists && !otherE.getKey().timestampsIntersect(baseClock)) {
                    s.put(otherE.getKey(), new HashSet<TripleTimestamp>(otherE.getValue()));
                    outputTimestampsToRegister.add(otherE.getKey());
                    outputTimestampsToRegister.addAll(otherE.getValue());
                }
                // else if !exists: otherE has been locally removed&pruned,
                // do not add it again
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
                if (ourInstance.getKey().timestampsIntersect(otherClock)
                        && !otherInstances.containsKey(ourInstance.getKey())) {
                    // ourInstance has been removed and pruned at the remote
                    // replica.
                    ourInstanceIter.remove();
                    outputTimestampsToUnregister.add(ourInstance.getKey());
                    outputTimestampsToUnregister.addAll(ourInstance.getValue());
                }
            }
        }
    }

    public static <V extends Object> List<TripleTimestamp> pruneImpl(
            Map<V, Map<TripleTimestamp, Set<TripleTimestamp>>> base, CausalityClock pruningPoint) {
        final List<TripleTimestamp> releasedTimestampUsages = new LinkedList<TripleTimestamp>();

        Iterator<Entry<V, Map<TripleTimestamp, Set<TripleTimestamp>>>> entries = base.entrySet().iterator();
        while (entries.hasNext()) {
            Entry<V, Map<TripleTimestamp, Set<TripleTimestamp>>> e = entries.next();
            Iterator<Map.Entry<TripleTimestamp, Set<TripleTimestamp>>> perClient = e.getValue().entrySet().iterator();
            while (perClient.hasNext()) {
                Entry<TripleTimestamp, Set<TripleTimestamp>> current = perClient.next();
                // TODO: implement Sergio's optimization - for adds, leave only
                // instances with the latest timestamp per DC.
                final Set<TripleTimestamp> removals = current.getValue();
                for (final TripleTimestamp removeTs : removals) {
                    if (removeTs.timestampsIntersect(pruningPoint)) {
                        perClient.remove();
                        releasedTimestampUsages.addAll(removals);
                        break;
                    }
                }
            }
            if (e.getValue().isEmpty()) {
                entries.remove();
            }
        }

        return releasedTimestampUsages;
    }

    public static <V extends Object> Map<V, Set<TripleTimestamp>> getValue(
            Map<V, Map<TripleTimestamp, Set<TripleTimestamp>>> base, CausalityClock snapshotClock) {
        Map<V, Set<TripleTimestamp>> retValues = new HashMap<V, Set<TripleTimestamp>>();
        Set<Entry<V, Map<TripleTimestamp, Set<TripleTimestamp>>>> entrySet = base.entrySet();
        for (Entry<V, Map<TripleTimestamp, Set<TripleTimestamp>>> e : entrySet) {
            Set<TripleTimestamp> present = new HashSet<TripleTimestamp>();
            for (Entry<TripleTimestamp, Set<TripleTimestamp>> p : e.getValue().entrySet()) {
                if (p.getKey().timestampsIntersect(snapshotClock)) {
                    boolean add = true;
                    for (TripleTimestamp remTs : p.getValue()) {
                        if (remTs.timestampsIntersect(snapshotClock)) {
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

    public static <V extends Comparable<V>> SortedMap<V, Set<TripleTimestamp>> getOrderedValue(
            SortedMap<V, Map<TripleTimestamp, Set<TripleTimestamp>>> base, CausalityClock snapshotClock) {
        
        SortedMap<V, Set<TripleTimestamp>> retValues = new TreeMap<V, Set<TripleTimestamp>>();
        Set<Entry<V, Map<TripleTimestamp, Set<TripleTimestamp>>>> entrySet = base.entrySet();
        for (Entry<V, Map<TripleTimestamp, Set<TripleTimestamp>>> e : entrySet) {
            Set<TripleTimestamp> present = new HashSet<TripleTimestamp>();
            for (Entry<TripleTimestamp, Set<TripleTimestamp>> p : e.getValue().entrySet()) {
                if (p.getKey().timestampsIntersect(snapshotClock)) {
                    boolean add = true;
                    for (TripleTimestamp remTs : p.getValue()) {
                        if (remTs.timestampsIntersect(snapshotClock)) {
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
