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

import swift.clocks.Timestamp;
import swift.crdt.interfaces.CRDTUpdate;
import swift.crdt.operations.BaseUpdate;

/**
 *
 * @author mehdi urso
 */
public class LogootInsert<V> extends BaseUpdate<LogootVersionned<V>> {
    
    final private LogootIdentifier identif;
    final private V content;

    public LogootInsert(LogootIdentifier identif, V content) {
        super(identif.getComponentAt(identif.length()-1).getTs());
        this.identif = identif;
        this.content = content;
    }

    public LogootIdentifier getIdentifiant() {
        return identif;
    }
    
    public V getContent() {
        return content;
    }

    @Override
    public CRDTUpdate<LogootVersionned<V>> withBaseTimestamp(Timestamp ts) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void replaceDependeeOperationTimestamp(Timestamp oldTs, Timestamp newTs) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void applyTo(LogootVersionned<V> crdt) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
