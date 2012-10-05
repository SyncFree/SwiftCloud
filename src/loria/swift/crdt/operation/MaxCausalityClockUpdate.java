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
package loria.swift.crdt.operation;

import loria.swift.crdt.MaxCausalityClock;
import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTUpdate;
import swift.crdt.operations.BaseUpdate;
import swift.crdt.operations.RegisterUpdate;

/**
 *
 * @author Stephane Martin <stephane.martin@loria.fr>
 */
public class MaxCausalityClockUpdate extends BaseUpdate {
 private CausalityClock c;
 private CausalityClock val;

    public MaxCausalityClockUpdate(CausalityClock c, CausalityClock val) {
        this.c = c;
        this.val = val;
    }

    public MaxCausalityClockUpdate(CausalityClock c, CausalityClock val, TripleTimestamp ts) {
        super(ts);
        this.c = c;
        this.val = val;
    }
 
    public MaxCausalityClockUpdate(TripleTimestamp ts, CausalityClock val, CausalityClock c) {
          super(ts);
        this.c = c;
        this.val = val;
    }

    @Override
    public CRDTUpdate withBaseTimestamp(Timestamp ts) {
        return new MaxCausalityClockUpdate(getTimestamp().withBaseTimestamp(ts), val, c);
    }

    @Override
    public void replaceDependeeOperationTimestamp(Timestamp oldTs, Timestamp newTs) {  
        // WISHME: extract as a CausalityClock method?
        if (c.includes(oldTs)) {
            c.drop(oldTs);
            c.record(newTs);
        }
        
    }

    @Override
    public void applyTo(CRDT crdt) {
        if (crdt instanceof MaxCausalityClock){
        ((MaxCausalityClock)crdt).update(val, getTimestamp(), c);
        }
       
    }

    public CausalityClock getVal() {
        return val;
    }
    
}
