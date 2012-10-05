/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package loria.swift.application.filesystem;

import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.Copyable;

/**
 *
 * @author urso
 */
public class Folder implements Copyable, FileSystemObject {
    String name;
    CRDTIdentifier setId;

    public Folder(String name, CRDTIdentifier setId) {
        this.name = name;
        this.setId = setId;
    }

    public Folder() {
    }
    
    
    @Override
    public Object copy() {
        return new Folder(name, setId);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getType() {
        return "folder";
    }
}
