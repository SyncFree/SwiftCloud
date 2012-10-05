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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.BaseCRDT;
import swift.crdt.interfaces.CRDTUpdate;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;

/**
 *
 * @author mehdi urso
 */
public class LogootVersionned<T> extends BaseCRDT<LogootVersionned<T>> {

    @Override
    protected void pruneImpl(CausalityClock pruningPoint) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    protected void mergePayload(LogootVersionned<T> otherObject) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    protected void execute(CRDTUpdate<LogootVersionned<T>> op) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    protected TxnLocalCRDT<LogootVersionned<T>> getTxnLocalCopyImpl(CausalityClock versionClock, TxnHandle txn) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    protected Set<Timestamp> getUpdateTimestampsSinceImpl(CausalityClock clock) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void rollback(Timestamp ts) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
