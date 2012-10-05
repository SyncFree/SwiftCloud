/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package loria.swift.application.filesystem;

import java.io.ByteArrayOutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import loria.swift.crdt.MaxCausalityClockTxnLocal;
import loria.swift.crdt.logoot.LogootVersionned;
import swift.clocks.CausalityClock;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.Copyable;
import swift.crdt.interfaces.TxnHandle;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

/**
 * File to be stored. Contains name and id of its content.
 *
 * @author urso
 * @author Stephane martin <stephane.martin@loria.fr>
 */
public class File extends FileSystemObject implements Copyable, Comparable<File> {

    @Override
    public Object copy() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int compareTo(File o) {
        return o == null ? 1 : this.pwd.compareTo(o.pwd);
    }

    @Override
    public void uptodate(TxnHandle txn) {
        this.txn=txn;
    }

    public static enum FileType {

        Logoot, Raw
    };
    transient CausalityClock wipeClock;
    transient ByteArrayOutputStream content;
    transient FileContent fc;
    // Class of maximun Causality Clock CRDT
    private static Class maxCCClass = MaxCausalityClockTxnLocal.class;
    // Class of file content CRDT
    private Class fileContentClass = LogootVersionned.class;

    public File(TxnHandle txn, String str) {
        super(txn, str);
    }
    /* void create(/*Class type*){
        
     }*/

    void delete() {
    }
    /*@Override
     public boolean isExisting(){
     throw new UnsupportedOperationException("not yet");
     }*/

    /*void load() {
        try {
            wipeClock = getWipeClock(txn);
        } catch (Exception ex) {
        }
    }*/

    public String getContent() {
        try {
            if (fc == null) {
                fc = getContent(txn, pwd, false);
            }
        } catch (WrongTypeException ex) {
            Logger.getLogger(File.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NoSuchObjectException ex) {
            return "";
        } catch (VersionNotFoundException ex) {
            Logger.getLogger(File.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NetworkException ex) {
            Logger.getLogger(File.class.getName()).log(Level.SEVERE, null, ex);
        }
        return fc.getText();
    }

    private CausalityClock getWipeClock(TxnHandle txn)
            throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
        return ((MaxCausalityClockTxnLocal) txn.get(NamingScheme.forWipeClock(this.pwd), false, maxCCClass)).getValue();
    }

    private FileContent getContent(TxnHandle txn, String filePath, CausalityClock wipeClock)
            throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
        return ((FileContent) txn.get(NamingScheme.forContent(filePath, wipeClock), false, fileContentClass));
    }

    private FileContent getContent(TxnHandle txn, String filePath, boolean create)
            throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
        return ((FileContent) txn.get(NamingScheme.forContent(filePath, null), create, fileContentClass));
    }

    public void update(String str) {

        try {
            if (fc == null) {
                fc = getContent(txn, pwd, true);
            }
        } catch (WrongTypeException ex) {
            Logger.getLogger(File.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NoSuchObjectException ex) {
        } catch (VersionNotFoundException ex) {
            Logger.getLogger(File.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NetworkException ex) {
            Logger.getLogger(File.class.getName()).log(Level.SEVERE, null, ex);
        }
        if (fc != null) {
            fc.update(str);
        }
    }

    @Override
    public String getType() {
        return "file";
    }
}
