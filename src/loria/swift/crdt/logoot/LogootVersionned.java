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
import java.util.Iterator;
import java.util.Set;
import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.BaseCRDT;
import swift.crdt.interfaces.CRDTUpdate;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;

/**
 * Logoot CRDT with versionning.
 * @author urso
 */
public class LogootVersionned extends BaseCRDT<LogootVersionned> {

    private LogootDocumentWithTombstones<String> doc;

    public LogootVersionned() {
        this.doc = new LogootDocumentWithTombstones<String>();
    }

    LogootDocumentWithTombstones<String> getDoc() {
        return doc;
    }

    @Override
    protected void mergePayload(LogootVersionned other) {
        doc.merge(other.doc);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof LogootVersionned)) {
            return false;
        }
        return doc.equals(((LogootVersionned)o).doc);
    }

    @Override
    public String toString() {
        return "{" + doc.idTable + " | " + doc.document + " | " + doc.tombstones + "}";
    }
    
    private static boolean greater(CausalityClock pruningPoint, Set<TripleTimestamp> tbs) {
        for (TripleTimestamp ts : tbs) {
            if (pruningPoint.includes(ts)) {
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
            }
        }
        newDoc.idTable.add(LogootDocument.end); // optimization 
        newDoc.document.add(null);
        newDoc.tombstones.add(null);
        doc = newDoc; 
    }

    // Not sure of exact usage.
    @Override
    protected Set<Timestamp> getUpdateTimestampsSinceImpl(CausalityClock clock) {
        final Set<Timestamp> result = new HashSet<Timestamp>();
        final int n = doc.size() - 1;
        
        for (int i = 1; i < n; ++i) {
            TripleTimestamp tins = doc.idTable.get(i).getLastComponent().getTs();
            if (!clock.includes(tins)) {
                result.add(tins.cloneBaseTimestamp());
            }
            Set<TripleTimestamp> tbs = doc.tombstones.get(i);
            if (tbs != null) {
                for (TripleTimestamp tdel : tbs) {
                    if (!clock.includes(tdel)) {
                        result.add(tdel.cloneBaseTimestamp());
                    }
                }
            }
        }
        return result;
    }


    @Override
    protected void execute(CRDTUpdate<LogootVersionned> op) {
        op.applyTo(this);
    }

    @Override
    protected TxnLocalCRDT<LogootVersionned> getTxnLocalCopyImpl(CausalityClock versionClock, TxnHandle txn) {
        final LogootVersionned creationState = isRegisteredInStore() ? null : new LogootVersionned();
        return new LogootTxnLocal(id, txn, versionClock, creationState, getValue(versionClock)); 
    }

    @Override
    public void rollback(Timestamp ts) {
        int i = 1;
        
        while (i < doc.size() - 1) {
            if (doc.idTable.get(i).getLastComponent().getTs().equals(ts)) {
                doc.remove(i);
            } else {
                Set<TripleTimestamp> tbs = doc.tombstones.get(i);
                if (tbs != null) {
                    Iterator<TripleTimestamp> it = tbs.iterator();
                    while (it.hasNext()) {
                        if (!it.next().equals(ts)) {
                            it.remove();
                        }
                    }
                }
                ++i;
            }
        }
    }

    LogootDocument getValue(CausalityClock versionClock) {
        final LogootDocument<String> view = new LogootDocument<String>();
        view.idTable.remove(1); // optimization 
        view.document.remove(1);
        int n = doc.idTable.size();
        
        for (int i = 1; i < n-1; ++i) {
            if (versionClock.includes(doc.idTable.get(i).getLastComponent().getTs())) {
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
