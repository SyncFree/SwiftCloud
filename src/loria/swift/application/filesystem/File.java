/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package loria.swift.application.filesystem;

import swift.clocks.CausalityClock;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.Copyable;

/**
 *
 * @author urso
 */
public class File implements Copyable, FileSystemObject {
    String name;
    CRDTIdentifier maxCCId;

    public File(String name, CRDTIdentifier maxCCId) {
        this.name = name;
        this.maxCCId = maxCCId;
    }

    public File() {
    }
    
    
    @Override
    public Object copy() {
        return new File(name, maxCCId);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getType() {
        return "file";
    }
}
