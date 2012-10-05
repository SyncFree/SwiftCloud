package loria.swift.application.filesystem.mapper;


import swift.clocks.CausalityClock;
import swift.crdt.*;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;
import swift.test.microbenchmark.objects.StringCopyable;

public class RegisterFileContent extends RegisterVersioned<StringCopyable> {

    public RegisterFileContent() {
    }

    private RegisterFileContent(RegisterFileContent other) {
        super(other);
    }

    @Override
    protected TxnLocalCRDT getTxnLocalCopyImpl(CausalityClock versionClock, TxnHandle txn) {
        final RegisterFileContent creationState = isRegisteredInStore() ? null : new RegisterFileContent();
        RegisterTxnFileContent localView = new RegisterTxnFileContent(id, txn, versionClock, creationState, value(versionClock));
        return localView;
    }

    @Override
    public RegisterFileContent copy() {
        return new RegisterFileContent(this);
    }
}
