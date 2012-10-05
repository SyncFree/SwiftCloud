/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package loria.swift.crdt;

import java.util.Set;
import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.BaseCRDT;
import swift.crdt.interfaces.CRDTUpdate;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;

/**
 *
 * @author urso
 */
public class MaxCausalityClockVersionned extends BaseCRDT {

    @Override
    protected void pruneImpl(CausalityClock pruningPoint) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    protected void mergePayload(BaseCRDT otherObject) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    protected void execute(CRDTUpdate op) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    protected TxnLocalCRDT getTxnLocalCopyImpl(CausalityClock versionClock, TxnHandle txn) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    protected Set getUpdateTimestampsSinceImpl(CausalityClock clock) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void rollback(Timestamp ts) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}
