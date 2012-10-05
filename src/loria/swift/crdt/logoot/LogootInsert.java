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
 * Insertion operation.
 * 
 * @author urso
 */
public class LogootInsert extends BaseUpdate<LogootVersioned> {

    private LogootIdentifier identif;
    private String content;

    public LogootInsert() {
    }

    public LogootInsert(LogootIdentifier identif, String content) {
        super(identif.getLastComponent().getTs());
        this.identif = identif;
        this.content = content;
    }

    public LogootIdentifier getIdentifiant() {
        return identif;
    }

    public String getContent() {
        return content;
    }

    @Override
    public void applyTo(LogootVersioned crdt) {
        crdt.applyInsert(identif, content);
    }
}
