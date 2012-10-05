/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package loria.swift.application.filesystem.mapper;

import loria.swift.application.filesystem.mapper.FileContent;
import swift.application.filesystem.StringCopyable;
import swift.clocks.CausalityClock;
import swift.crdt.CRDTIdentifier;
import swift.crdt.RegisterTxnLocal;
import swift.crdt.RegisterVersioned;
import swift.crdt.interfaces.TxnHandle;

/**
 * Rhough file content using register (no merge!).
 * @author urso
 */
public class RegisterTxnFileContent extends RegisterTxnLocal<StringCopyable> implements FileContent {

    public RegisterTxnFileContent(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, RegisterVersioned<StringCopyable> creationState, StringCopyable val) {
        super(id, txn, clock, creationState, val);
    }

    @Override
    public void set(String newValue) {
        set(new StringCopyable(newValue));
    }

    @Override
    public String getText() {
        return getValue().getString();
    }
}
