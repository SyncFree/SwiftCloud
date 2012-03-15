package swift.crdt;

import swift.clocks.CausalityClock;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;

public class SetIntegers extends SetVersioned<Integer, SetIntegers> {

    @Override
    public TxnLocalCRDT<SetIntegers> getTxnLocalCopy(CausalityClock pruneClock, CausalityClock versionClock,
            TxnHandle txn) {

        SetTxnLocalInteger localView = new SetTxnLocalInteger(id, txn, versionClock, getValue(versionClock));
        return (TxnLocalCRDT<SetIntegers>) localView;
    }

}
