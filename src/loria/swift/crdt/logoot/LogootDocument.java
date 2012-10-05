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
 * @author urso mehdi
 */
public class LogootDocument<T> {    
    final protected RangeList<LogootIdentifier> idTable;
    final protected RangeList<T> document;

    public LogootDocument() {
        this.idTable = new RangeList<LogootIdentifier>();
        this.document = new RangeList<T>();
        
        LogootIdentifier Begin = new LogootIdentifier(1), End = new LogootIdentifier(1);
        Begin.addComponent(new Component(0, new TripleTimestamp()));
        End.addComponent(new Component(LogootTxnLocal.max, new TripleTimestamp()));

        idTable.add(Begin);
        document.add(null);
        idTable.add(End);
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
}
