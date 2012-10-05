/*
 *  Replication Benchmarker
 *  https://github.com/score-team/replication-benchmarker/
 *  Copyright (C) 2012 LORIA / Inria / SCORE Team
 * 
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 * 
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 * 
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.

 */
package loria.swift.crdt;

import swift.clocks.CausalityClock;
import swift.crdt.BaseCRDTTxnLocal;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTQuery;
import swift.crdt.interfaces.TxnHandle;

/**
 *
 * @author Stephane Martin <stephane.martin@loria.fr>
 */
public class MaxCausalityClockTxnLocal extends BaseCRDTTxnLocal {

    public MaxCausalityClockTxnLocal(CausalityClock vc, CRDTIdentifier id, TxnHandle txn, CausalityClock clock, CRDT creationState) {
        super(id, txn, clock, creationState);
        this.vc = vc;
    }
    
    CausalityClock vc;
    @Override
    public CausalityClock getValue() {
        return vc;
    }
    public void setValue(CausalityClock vc){
        this.vc=vc;
    }
    @Override
    public Object executeQuery(CRDTQuery query) {
        return query.executeAt(this);
    }

}
