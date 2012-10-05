/**
 * Replication Benchmarker
 * https://github.com/score-team/replication-benchmarker/
 * Copyright (C) 2012 LORIA / Inria / SCORE Team
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package loria.swift.crdt.logoot;

import java.util.HashSet;
import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.BaseCRDT;
import swift.crdt.interfaces.CRDTUpdate;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;

/**
 * Logoot CRDT with versionning.
 * 
 * @author urso
 */
public class LogootVersioned extends BaseCRDT<LogootVersioned> {

    private LogootDocumentWithTombstones<String> doc;

    public LogootVersioned() {
        this.doc = new LogootDocumentWithTombstones<String>();
    }

    LogootDocumentWithTombstones<String> getDoc() {
        return doc;
    }

    @Override
    protected void mergePayload(LogootVersioned other) {
        int i = 1, j = 1, tj = other.doc.idTable.size() - 1;
        while (j < tj) {
            int comp = doc.idTable.get(i).compareTo(other.doc.idTable.get(j));
            if (comp > 0) {
                // Add new stuff, but do not introduce pruned positions.
                if (!greater(getClock(), other.doc.tombstones.get(j))) {
                    doc.add(i, other.doc.idTable.get(j), other.doc.document.get(j), other.doc.tombstones.get(j));
                    registerTimestampUsage(other.doc.idTable.get(j).getLastComponent().getTs());
                    final Set<TripleTimestamp> tombstoneTimestamps = other.doc.tombstones.get(j);
                    if (tombstoneTimestamps != null) {
                        for (final TripleTimestamp tombstoneTs : tombstoneTimestamps) {
                            registerTimestampUsage(tombstoneTs);
                        }
                    }
                }
                ++j;
            } else if (comp == 0) {
                Set<TripleTimestamp> ot = other.doc.tombstones.get(j);
                if (ot != null) {
                    Set<TripleTimestamp> ourTombstones = doc.tombstones.get(i);
                    if (ourTombstones == null) {
                        ourTombstones = new HashSet<TripleTimestamp>();
                        doc.tombstones.set(i, ourTombstones);
                    }
                    for (final TripleTimestamp tombstoneTs : ot) {
                        if (ourTombstones.add(tombstoneTs)) {
                            registerTimestampUsage(tombstoneTs);
                        }
                    }
                }
                ++j;
            }
            ++i;
        }
    }

    @Override
    public String toString() {
        return "{" + doc.idTable + " | " + doc.document + " | " + doc.tombstones + "}";
    }

    private static boolean greater(CausalityClock pruningPoint, Set<TripleTimestamp> tbs) {
        for (TripleTimestamp ts : tbs) {
            if (ts.timestampsIntersect(pruningPoint)) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected void pruneImpl(CausalityClock pruningPoint) {
        final LogootDocumentWithTombstones<String> newDoc = new LogootDocumentWithTombstones<String>();
        newDoc.idTable.remove(1); // optimization
        newDoc.document.remove(1);
        newDoc.tombstones.remove(1);
        int n = doc.idTable.size() - 1;

        for (int i = 1; i < n; ++i) {
            Set<TripleTimestamp> tbs = doc.tombstones.get(i);
            if (tbs == null || !greater(pruningPoint, tbs)) {
                newDoc.idTable.add(doc.idTable.get(i));
                newDoc.document.add(doc.document.get(i));
                newDoc.tombstones.add(doc.tombstones.get(i));
            } else {
                unregisterTimestampUsage(doc.idTable.get(i).getLastComponent().getTs());
                if (doc.tombstones.get(i) != null) {
                    for (final TripleTimestamp removeTs : doc.tombstones.get(i)) {
                        unregisterTimestampUsage(removeTs);
                    }
                }
            }
        }
        newDoc.idTable.add(LogootDocument.end); // optimization
        newDoc.document.add(null);
        newDoc.tombstones.add(null);
        doc = newDoc;
    }

    @Override
    protected void execute(CRDTUpdate<LogootVersioned> op) {
        op.applyTo(this);
    }

    void applyInsert(final LogootIdentifier id, final String content) {
        doc.insert(id, content);
        registerTimestampUsage(id.getLastComponent().getTs());
    }

    void applyDelete(final LogootIdentifier removedId, TripleTimestamp ts) {
        doc.delete(removedId, ts);
        registerTimestampUsage(ts);
    }

    @Override
    protected TxnLocalCRDT<LogootVersioned> getTxnLocalCopyImpl(CausalityClock versionClock, TxnHandle txn) {
        final LogootVersioned creationState = isRegisteredInStore() ? null : new LogootVersioned();
        return new LogootTxnLocal(id, txn, versionClock, creationState, getValue(versionClock));
    }

    LogootDocument getValue(CausalityClock versionClock) {
        final LogootDocument<String> view = new LogootDocument<String>();
        view.idTable.remove(1); // optimization
        view.document.remove(1);
        int n = doc.idTable.size();

        for (int i = 1; i < n - 1; ++i) {
            if (doc.idTable.get(i).getLastComponent().getTs().timestampsIntersect(versionClock)) {
                Set<TripleTimestamp> tbs = doc.tombstones.get(i);
                if (tbs == null || !greater(versionClock, tbs)) {
                    view.idTable.add(doc.idTable.get(i));
                    view.document.add(doc.document.get(i));
                }
            }
        }
        view.idTable.add(LogootDocument.end);
        view.document.add(null);
        return view;
    }

    void setDoc(LogootDocumentWithTombstones<String> x) {
        doc = x;
    }
}
