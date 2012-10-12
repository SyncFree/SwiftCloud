/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package loria.swift.application.filesystem;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;
import loria.swift.application.filesystem.mapper.FileContent;
import loria.swift.crdt.logoot.LogootVersioned;
import swift.application.filesystem.IFile;
import swift.clocks.CausalityClock;
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
public class File extends FileSystemObject implements Copyable, Comparable<File>,IFile {

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

    

    @Override
    public void update(ByteBuffer buf, long offset) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void reset(byte[] data) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void read(ByteBuffer buf, long offset) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public byte[] get(int offset, int length) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public byte[] getBytes() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getSize() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public static enum FileType {

        Logoot, Raw
    };
    transient CausalityClock wipeClock;
    transient ByteArrayOutputStream content;
    transient FileContent fc;
    // Class of maximun Causality Clock CRDT
    //private static Class maxCCClass = MaxCausalityClockTxnLocal.class;
    // Class of file content CRDT
    private Class fileContentClass = LogootVersioned.class;

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
        throw new UnsupportedOperationException(); 
        //return ((MaxCausalityClockTxnLocal) txn.get(NamingScheme.forWipeClock(this.pwd), false, maxCCClass)).getValue();
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
            fc.set(str);
        }
    }

    @Override
    public String getType() {
        return "file";
    }
}
