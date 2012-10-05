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
 * @author Stephane Martin <stephane.martin@loria.fr>
 */
class NamingScheme {
    public static final String FILES="files";
    public static final String FOLDERS="folders";
    public static final String CONTENT="content";
    public static final String WHIPECLOCK="whipeclock";
    
    
    
    public static CRDTIdentifier forFile(final String filePath) {
        return new CRDTIdentifier(FILES, filePath);
    }
    
    public static CRDTIdentifier forFolder(final String filePath) {
        return new CRDTIdentifier(FOLDERS, filePath);
    }
    
   /* public static CRDTIdentifier forTree() {
        return new CRDTIdentifier(FOLDERS, "/");
    }*/
    
    public static CRDTIdentifier forContent(final String filePath, final CausalityClock clock) {
        return new CRDTIdentifier(CONTENT, filePath + ((clock==null)?("@" + clock.toString()):""));
    }

    static CRDTIdentifier forWipeClock(String pwd) {
        return new CRDTIdentifier(WHIPECLOCK, pwd);
    }
}
