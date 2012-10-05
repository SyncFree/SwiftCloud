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
import java.util.List;
import java.util.Set;
import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;

/**
 * A Logoot document with tombstones. Each tripletimestamp associated to a logoot 
 * identifier correspond to a delete.  
 * @author urso 
 */
public class LogootDocumentWithTombstones<T> extends LogootDocument<T> {    
    final protected RangeList<Set<TripleTimestamp>> tombstones;

    public LogootDocumentWithTombstones() {
        super();
        this.tombstones = new RangeList<Set<TripleTimestamp>>();
        tombstones.add(null);
        tombstones.add(null);
    }
    
    void add(int pos, LogootIdentifier id, T content, Set<TripleTimestamp> tbs) {
        idTable.add(pos, id);
        document.add(pos, content);
        tombstones.add(pos, tbs);
    }
    
    public void insert(LogootIdentifier id, T content) {
        int pos = dicho(id);
        add(pos, id, content, null);
    }   

    public void delete(LogootIdentifier id, TripleTimestamp ts) {
        int pos = dicho(id);
        Set<TripleTimestamp> tbs = tombstones.get(pos);
        if (tbs == null) {
            tbs = new HashSet<TripleTimestamp>();
            tombstones.set(pos, tbs);
        }
        tbs.add(ts);
    }

    void merge(LogootDocumentWithTombstones<T> other) {
        int i = 1, j = 1, tj = other.idTable.size() - 1;
        while (j < tj) {
            int comp = idTable.get(i).compareTo(other.idTable.get(j));
            if (comp > 0) {
                add(i, other.idTable.get(j), other.document.get(j), other.tombstones.get(j));
                ++j;
            } else if (comp == 0) {
                Set<TripleTimestamp> ot = other.tombstones.get(j);
                if (ot != null) {
                    if (tombstones.get(i) == null) {
                        tombstones.set(i, ot);
                    } else {
                        tombstones.get(i).addAll(ot);
                    }
                }
                ++j;
            } 
            ++i;
        }
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 1 ; i < document.size()-1; ++i) {
            if (tombstones.get(i) == null || tombstones.get(i).isEmpty()) {  // could it be empty ?
                sb.append(document.get(i)).append('\n');
            }
        }
        return sb.toString();
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 37 * hash + (this.tombstones != null ? this.tombstones.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final LogootDocumentWithTombstones<T> other = (LogootDocumentWithTombstones<T>) obj;
        if (this.tombstones != other.tombstones && (this.tombstones == null || !this.tombstones.equals(other.tombstones))) {
            return false;
        }
        return super.equals(obj);
    }
}
