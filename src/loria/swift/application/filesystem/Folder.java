/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package loria.swift.application.filesystem;

import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.Copyable;

/**
 * Folder to be stored. Contains name and id of its content.
 * @author urso
 */
public class Folder implements Copyable, FileSystemObject {
    final String name;
    final CRDTIdentifier setId;

    public Folder(String name, CRDTIdentifier setId) {
        this.name = name;
        this.setId = setId;
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
