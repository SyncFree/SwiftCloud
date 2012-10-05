/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package loria.swift.application.filesystem;

import swift.clocks.CausalityClock;
import swift.crdt.CRDTIdentifier;

/**
 *
 * @author urso
 */
class NamingScheme {
    public static CRDTIdentifier forFile(final String filePath) {
        return new CRDTIdentifier("files", filePath);
    }
    
    
    public static CRDTIdentifier forContent(final String filePath, final CausalityClock clock) {
        return new CRDTIdentifier("content", filePath + "@" + clock.toString());
    }
}
