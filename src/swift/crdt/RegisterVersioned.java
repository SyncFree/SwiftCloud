package swift.crdt;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import swift.clocks.CausalityClock;
import swift.clocks.ClockFactory;
import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDTUpdate;
import swift.crdt.interfaces.Copyable;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;

public class RegisterVersioned<V extends Copyable> extends BaseCRDT<RegisterVersioned<V>> {
    private static final long serialVersionUID = 1L;

    public static class UpdateEntry<V extends Copyable> implements Comparable<UpdateEntry<V>>, Serializable {
        private static final long serialVersionUID = 4540422641079766746L;

        long lamportClock;
        TripleTimestamp ts;
        V value;

        // Kryo USE ONLY.
        UpdateEntry() {
        }

        UpdateEntry(long lamportClock, final TripleTimestamp ts, V value) {
            this.lamportClock = lamportClock;
            this.ts = ts;
            this.value = value;
        }

        @Override
        public int compareTo(UpdateEntry<V> o) {
            if (lamportClock != o.lamportClock) {
                return Long.signum(o.lamportClock - lamportClock);
            }
            return ts.compareTo(o.ts);
        }

        @SuppressWarnings({ "unchecked" })
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof UpdateEntry)) {
                return false;
            }
            return compareTo((UpdateEntry<V>) obj) == 0;
        }

        @SuppressWarnings("unchecked")
        public UpdateEntry<V> copy() {
            return new UpdateEntry<V>(lamportClock, ts, (V) value.copy());
        }
        
        @Override
        public String toString() {
            return value.toString();
        }
    }

    // List of register updates, the order is a deterministic linear extension
    // of causal dependency relation. Recent update comes first.
    private SortedSet<UpdateEntry<V>> values;

    public RegisterVersioned() {
        this.values = new TreeSet<UpdateEntry<V>>();
    }

    @Override
    public void rollback(Timestamp ts) {
        // FIXME: deal with mappings
        final CausalityClock tsAsClock = ClockFactory.newClock();
        tsAsClock.record(ts);

        final Iterator<UpdateEntry<V>> it = values.iterator();
        while (it.hasNext()) {
            UpdateEntry<V> entry = it.next();
            if (entry.ts.timestampsIntersect(tsAsClock)) {
                it.remove();
            }
        }
    }

    @Override
    protected void pruneImpl(CausalityClock pruningPoint) {
        // Short cut for objects that are rarely updated: If there is only one
        // value, it must remain
        if (values.size() == 1) {
            return;
        }

        // Remove all values older than the pruningPoint, except the single
        // value representing purningPoint - there must be a summary of pruned
        // state.
        boolean firstMatchSkipped = false;
        final Iterator<UpdateEntry<V>> iter = values.iterator();
        while (iter.hasNext()) {
            final UpdateEntry<V> entry = iter.next();
            if (entry.ts.timestampsIntersect(pruningPoint)) {
                if (firstMatchSkipped) {
                    unregisterTimestampUsage(entry.ts);
                    iter.remove();
                } else {
                    firstMatchSkipped = true;
                }
            }
        }
    }

    public void update(long lamportClock, TripleTimestamp updateTimestamp, V val) {
        values.add(new UpdateEntry<V>(lamportClock, updateTimestamp, val));
        registerTimestampUsage(updateTimestamp);
    }

    @Override
    protected void mergePayload(RegisterVersioned<V> otherObject) {
        for (final UpdateEntry<V> entry : otherObject.values) {
            if (values.add(entry)) {
                registerTimestampUsage(entry.ts);
            }
        }
    }

    @Override
    protected TxnLocalCRDT<RegisterVersioned<V>> getTxnLocalCopyImpl(CausalityClock versionClock, TxnHandle txn) {
        final RegisterVersioned<V> creationState = isRegisteredInStore() ? null : new RegisterVersioned<V>();
        final UpdateEntry<V> value = value(versionClock);
        if (value != null) {
            return new RegisterTxnLocal<V>(id, txn, versionClock, creationState, value.value, value.lamportClock + 1);
        } else {
            return new RegisterTxnLocal<V>(id, txn, versionClock, creationState, null, 0);
        }
    }

    private UpdateEntry<V> value(CausalityClock versionClock) {
        for (UpdateEntry<V> e : values) {
            if (e.ts.timestampsIntersect(versionClock)) {
                return e;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return values.toString();
    }

    @Override
    protected void execute(CRDTUpdate<RegisterVersioned<V>> op) {
        op.applyTo(this);
    }

    public RegisterVersioned<V> copy() {
        RegisterVersioned<V> copyObj = new RegisterVersioned<V>();
        for (UpdateEntry<V> e : values) {
            copyObj.values.add(e.copy());
        }
        copyBase(copyObj);
        return copyObj;
    }

    // TODO(Marek): The following piece of code is useful if one wants to
    // implement MV+LWWRegister but requires an extra bit of work to implement
    // pruning.
    //
    // /**
    // * A node of a causal history DAG with a value of type V.
    // *
    // * @author zawir
    // * @param <V>
    // */
    // public static class DAGNode<V extends Copyable> implements
    // Comparable<DAGNode<V>> {
    // UpdateTimestamp ts;
    // V value;
    // List<DAGNode<V>> successors;
    //
    // public DAGNode() {
    // }
    //
    // public DAGNode(final UpdateTimestamp ts, V value) {
    // this.ts = ts;
    // this.value = value;
    // this.successors = new LinkedList<DAGNode<V>>();
    // }
    //
    // public void addSuccessor(final DAGNode<V> successor) {
    // successors.add(successor);
    // }
    //
    // public Collection<DAGNode<V>> getLatestNodes(final CausalityClock
    // visibleClock) {
    // final HashSet<UpdateTimestamp> visitedIds = new
    // HashSet<UpdateTimestamp>();
    // final HashMap<UpdateTimestamp, DAGNode<V>> latestNodes = new
    // HashMap<UpdateTimestamp, DAGNode<V>>();
    // if (!ts.timestampsIntersect(visibleClock)) {
    // throw new IllegalArgumentException(
    // "The provided visibleClock does not include the root of causal history");
    // }
    // getLatestNodesRecursive(visitedIds, latestNodes, visibleClock);
    // return latestNodes.values();
    // }
    //
    // public DAGNode<V> getLatestNode(final CausalityClock visibleClock) {
    // final Collection<DAGNode<V>> latestNodes = getLatestNodes(visibleClock);
    // }
    //
    // /**
    // *
    // * @param visitedIds
    // * @param latestNodes
    // * @return true when the successor is valid
    // */
    // private boolean getLatestNodesRecursive(HashSet<UpdateTimestamp>
    // visitedIds,
    // HashMap<UpdateTimestamp, DAGNode<V>> latestNodes, final CausalityClock
    // visibleClock) {
    // if (!ts.timestampsIntersect(visibleClock)) {
    // return false;
    // }
    //
    // if (visitedIds.add(ts)) {
    // boolean foundSucc = false;
    // for (final DAGNode<V> succ : successors) {
    // if (succ.getLatestNodesRecursive(visitedIds, latestNodes, visibleClock))
    // {
    // foundSucc = true;
    // }
    // }
    // if (!foundSucc) {
    // latestNodes.put(ts, this);
    // }
    // }
    // return true;
    // }
    //
    // public V getValue() {
    // return value;
    // }
    //
    // /**
    // * Total-order comparator based on unique update id.
    // */
    // @Override
    // public int compareTo(DAGNode<V> o) {
    // return ts.compareTo(o.ts);
    // }
    // }
}
