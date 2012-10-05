/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package loria.swift.application.filesystem.mapper;

import loria.swift.application.filesystem.FileContent;
import swift.clocks.CausalityClock;
import swift.crdt.CRDTIdentifier;
import swift.crdt.RegisterTxnLocal;
import swift.crdt.RegisterVersioned;
import swift.crdt.interfaces.TxnHandle;

/**
 * Rhough file content using register (no merge!).
 * @author urso
 */
public class RegisterFileContent extends RegisterTxnLocal<Content> implements FileContent {

    public RegisterFileContent(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, RegisterVersioned<Content> creationState, Content val) {
        super(id, txn, clock, creationState, val);
    }

    @Override
    public void update(String newValue) {
        set(new Content(newValue));
    }

    @Override
    public String getText() {
        return getValue().content;
    }
}
