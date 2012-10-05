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

import java.util.Iterator;
import swift.clocks.CausalityClock;
import swift.clocks.ClockFactory;
import swift.crdt.RegisterVersioned;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;

/**
 *
 * @author Stephane Martin <stephane.martin@loria.fr>
 */
public class MaxCausalityClockRegister extends RegisterVersioned<CausalityClock> implements MaxCausalityClock {

    //protected boolean modified = true;
    CausalityClock base = ClockFactory.newClock();
    /*  @Override
     public void rollback(Timestamp ts) {
     modified = true;
     super.rollback(ts);
     }*/

    public MaxCausalityClockRegister() {
        super();
    }

    @Override
    protected void pruneImpl(CausalityClock pruningPoint) {
        base = this.value(pruningPoint);
        //super.pruneImpl(pruningPoint);
        final Iterator<QueueEntry<CausalityClock>> iter = values.iterator();
        while (iter.hasNext()) {
            if (pruningPoint.includes(iter.next().getTs())) {
                iter.remove();
            }
        }
    }

    /*@Override
     public void update(CausalityClock val, TripleTimestamp ts, CausalityClock c) {
     modified = true;
     super.update(val, ts, c);
     }*/
    @Override
    protected void mergePayload(RegisterVersioned<CausalityClock> otherObject) {

        MaxCausalityClockRegister obj = (MaxCausalityClockRegister) otherObject;
        super.mergePayload(otherObject);
        this.base.merge(obj.base);
    }

    @Override
    protected TxnLocalCRDT getTxnLocalCopyImpl(CausalityClock versionClock, TxnHandle txn) {
        //return super.getTxnLocalCopyImpl(versionClock, txn);

        final RegisterVersioned creationState = isRegisteredInStore() ? null : new RegisterVersioned();
        MaxCausalityClockTxnLocal localview = new MaxCausalityClockTxnLocal(id, txn, versionClock, creationState, (CausalityClock) value(versionClock));
        return localview;


    }

    @Override
    protected CausalityClock value(CausalityClock versionClock) {
        CausalityClock cc = (CausalityClock) base.copy();
        for (QueueEntry e : values) {
            if (versionClock.includes(e.getTs())) {
                cc.merge((CausalityClock)e.getValue());
             //   cc = cc == null ? (CausalityClock) e.getValue() : cc.max((CausalityClock) e.getValue());
            }
        }
        return cc;
    }

    /* @Override
     public String toString() {
     return super.toString();
     }*/

    /*@Override
     protected void execute(CRDTUpdate<RegisterVersioned<CausalityClock>> op) {
     op.applyTo(this);
     }*/
    @Override
    public MaxCausalityClockRegister copy() {
        MaxCausalityClockRegister copyObj = new MaxCausalityClockRegister();
        copyObj.base = this.base;
        for (QueueEntry<CausalityClock> e : values) {
            copyObj.values.add(e.copy());
        }
        copyBase(copyObj);
        return copyObj;
    }

    /*    @Override
     protected Set<Timestamp> getUpdateTimestampsSinceImpl(CausalityClock clock) {
     return super.getUpdateTimestampsSinceImpl(clock);
     }*/
}
