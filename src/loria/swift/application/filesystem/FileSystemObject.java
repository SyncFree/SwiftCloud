/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package loria.swift.application.filesystem;

import swift.crdt.interfaces.TxnHandle;

/**
 *
 * @author urso
 * @author Stephane Martin <stephane.martin@loria.fr>
 */
public abstract class FileSystemObject {

    final protected String pwd;
    protected TxnHandle txn;
    private Folder parent;

    public Folder getParent() {
        return parent;
    }

    void setParent(Folder parent) {
        this.parent = parent;
    }

    /**
     * @param txn the value of txn
     * @param pwd the value of pwd
     */
    public FileSystemObject(TxnHandle txn, String pwd) {
        this.pwd = pwd;
        this.txn = txn;
    }

    public TxnHandle getTxn() {
        return txn;
    }

    public String getName() {
        int index = pwd.lastIndexOf("/");
        return pwd.substring(index);
    }

    public String getPwd() {
        return pwd;
    }

    abstract String getType();

    protected String getParentFromAbs(String path) {
        int index = path.lastIndexOf('/');
        if (index < 1) {
            return "/";
        }
        return path.substring(0, index);
    }

    protected String convert2Abs(String path) {
        if (path.startsWith("/")) {
            return path;
        } else {
            return pwd + "/" + path;
        }
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 59 * hash + (this.pwd != null ? (this.getType() + this.pwd).hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final FileSystemObject other = (FileSystemObject) obj;
        if ((this.pwd == null) ? (other.pwd != null) : !this.pwd.equals(other.pwd)) {
            return false;
        }
        if ((this.getType() == null) ? (other.getType() == null) : !this.getType().equals(other.getType())) {
            return false;
        }
        return true;
    }
        public abstract void uptodate(TxnHandle txn);
    //public abstract boolean isExisting();
}
