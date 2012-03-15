package swift.crdt;

import swift.clocks.CausalityClock;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;

public class SetStrings extends SetVersioned<String, SetStrings> {

    @Override
    public TxnLocalCRDT<SetStrings> getTxnLocalCopy(CausalityClock pruneClock, CausalityClock versionClock,
            TxnHandle txn) {

        SetTxnLocalString localView = new SetTxnLocalString(id, txn, versionClock, getValue(versionClock));
        return (TxnLocalCRDT<SetStrings>) localView;
    }

}
