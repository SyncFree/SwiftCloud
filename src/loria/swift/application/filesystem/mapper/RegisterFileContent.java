package loria.swift.application.filesystem.mapper;

import swift.clocks.CausalityClock;
import swift.crdt.RegisterVersioned;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;
import swift.test.microbenchmark.objects.StringCopyable;

public class RegisterFileContent extends RegisterVersioned<StringCopyable> {

    public RegisterFileContent() {
    }

    @Override
    protected TxnLocalCRDT getTxnLocalCopyImpl(CausalityClock versionClock, TxnHandle txn) {
        final RegisterFileContent creationState = isRegisteredInStore() ? null : new RegisterFileContent();
        final UpdateEntry<StringCopyable> value = value(versionClock);
        if (value != null) {
            return new RegisterTxnFileContent(id, txn, versionClock, creationState, value.getValue(),
                    value.getLamportClock() + 1);
        } else {
            return new RegisterTxnFileContent(id, txn, versionClock, creationState, null, 0);
        }
    }

    @Override
    public RegisterFileContent copy() {
        final RegisterFileContent copy = new RegisterFileContent();
        copyLoad(copy);
        copyBase(copy);
        return copy;
    }
}
