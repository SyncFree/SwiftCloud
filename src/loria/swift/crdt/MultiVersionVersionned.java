package loria.swift.crdt;

import java.util.Collection;
import java.util.Set;
import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.BaseCRDT;
import swift.crdt.interfaces.CRDTUpdate;
import swift.crdt.interfaces.Copyable;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;

public class MultiVersionVersionned<V extends Copyable> extends BaseCRDT<MultiVersionVersionned<V>> {
    public void update(Collection<TripleTimestamp> old, V val, TripleTimestamp ts, CausalityClock c) {
    }

    @Override
    protected void pruneImpl(CausalityClock pruningPoint) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    protected void mergePayload(MultiVersionVersionned<V> otherObject) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    protected void execute(CRDTUpdate<MultiVersionVersionned<V>> op) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    protected TxnLocalCRDT<MultiVersionVersionned<V>> getTxnLocalCopyImpl(CausalityClock versionClock, TxnHandle txn) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
