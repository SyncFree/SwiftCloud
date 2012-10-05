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

import java.util.List;
import swift.clocks.TripleTimestamp;

/**
 * A Logoot document. Contains a list of Charater and the corresponding list of LogootIndentitifer.
 * @author urso 
 */
public class LogootDocument<T> {    
    final protected RangeList<LogootIdentifier> idTable;
    final protected RangeList<T> document;
    public final static LogootIdentifier begin = new LogootIdentifier(new Component(0, new TripleTimestamp())),
            end = new LogootIdentifier(new Component(0, new TripleTimestamp()));
    
    public LogootDocument() {
        this.idTable = new RangeList<LogootIdentifier>();
        this.document = new RangeList<T>();

        idTable.add(begin);
        idTable.add(end);
        document.add(null);
        document.add(null);
    }
    
    public void insert(int position, List<LogootIdentifier> patch, List<T> lc) {
        idTable.addAll(position + 1, patch);
        document.addAll(position + 1, lc);
    }
    
    public void delete(int position, int offset) {
        idTable.removeRangeOffset(position + 1, offset);
        document.removeRangeOffset(position + 1, offset);
    }
        
    
    protected int dicho(LogootIdentifier idToSearch) {
        int startIndex = 1, endIndex = idTable.size() - 1, middleIndex;
        while (startIndex < endIndex) {
            middleIndex = startIndex + (endIndex - startIndex) / 2;
            int c = idTable.get(middleIndex).compareTo(idToSearch);
            if (c == 0) {
                return middleIndex;
            } else if (c < 0) {
                startIndex = middleIndex + 1;
            } else {
                endIndex = middleIndex;
            }
        } 
        return startIndex;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 1 ; i < document.size()-1; ++i) {
            sb.append(document.get(i)).append('\n');
        }
        return sb.toString();
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 83 * hash + (this.idTable != null ? this.idTable.hashCode() : 0);
        hash = 83 * hash + (this.document != null ? this.document.hashCode() : 0);
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
        final LogootDocument<T> other = (LogootDocument<T>) obj;
        if (this.idTable != other.idTable && (this.idTable == null || !this.idTable.equals(other.idTable))) {
            return false;
        }
        if (this.document != other.document && (this.document == null || !this.document.equals(other.document))) {
            return false;
        }
        return true;
    }
}
