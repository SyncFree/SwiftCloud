package swift.application.filesystem;

import java.io.File;
import java.util.Collection;

import loria.swift.crdt.logoot.LogootVersioned;
import swift.crdt.CRDTIdentifier;
import swift.crdt.DirectoryTxnLocal;
import swift.crdt.DirectoryVersioned;
import swift.crdt.RegisterVersioned;
import swift.crdt.interfaces.TxnGetterSetter;
import swift.crdt.interfaces.TxnHandle;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;
import swift.utils.Pair;

public class FilesystemBasic implements Filesystem {
    // table in DHT that holds all entries for this filesystem

    private String table;
    // root directory
    private CRDTIdentifier root;

    // private final Class fileContentClass;

    public FilesystemBasic(TxnHandle txn, String root, String table) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        this.table = table;
        this.root = DirectoryTxnLocal.createRootId(table, root, DirectoryVersioned.class);
        txn.get(this.root, true, DirectoryVersioned.class);
        // this.fileContentClass = fileContentClass;
    }

    private static Class getFileClass(String name) {
        if (name.endsWith(".txt")) {
            return LogootVersioned.class;
        }
        return RegisterVersioned.class;
    }

    @Override
    public IFile createFile(TxnHandle txn, String fname, String path) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException, ClassNotFoundException {

        String pathToParent = DirectoryTxnLocal.getPathToParent(new CRDTIdentifier(table, path));
        String parent = DirectoryTxnLocal.getEntryName(new CRDTIdentifier(table, path));

        CRDTIdentifier parentId = DirectoryTxnLocal.getCRDTIdentifier(table, pathToParent, parent,
                DirectoryVersioned.class);
        DirectoryTxnLocal parentDir = txn.get(parentId, false, DirectoryVersioned.class);
        CRDTIdentifier fileId = parentDir.createNewEntry(fname, getFileClass(fname));
        TxnGetterSetter<Blob> fileContent = (TxnGetterSetter<Blob>) txn.get(fileId, true, getFileClass(fname));
        Blob initialFileContent = new Blob();
        fileContent.set(initialFileContent);
        return new FilePaged(initialFileContent);
    }

    @Override
    public IFile readFile(TxnHandle txn, String fname, String path) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {

        CRDTIdentifier fileId = DirectoryTxnLocal.getCRDTIdentifier(table, path, fname, getFileClass(fname));
        TxnGetterSetter<Blob> fileContent = (TxnGetterSetter<Blob>) txn.get(fileId, false, getFileClass(fname));
        return new FilePaged(fileContent.getValue());
    }

    @Override
    public void updateFile(TxnHandle txn, String fname, String path, IFile f) throws WrongTypeException,
            NoSuchObjectException, VersionNotFoundException, NetworkException {
        CRDTIdentifier fileId = DirectoryTxnLocal.getCRDTIdentifier(table, path, fname, getFileClass(fname));
        TxnGetterSetter<Blob> content = (TxnGetterSetter<Blob>) txn.get(fileId, false, getFileClass(fname));
        content.set(new Blob(f.getBytes()));
    }

    @Override
    public void removeFile(TxnHandle txn, String fname, String path) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException, ClassNotFoundException {
        String pathToParent = DirectoryTxnLocal.getPathToParent(new CRDTIdentifier(table, path));
        String parent = DirectoryTxnLocal.getEntryName(new CRDTIdentifier(table, path));

        CRDTIdentifier parentId = DirectoryTxnLocal.getCRDTIdentifier(table, pathToParent, parent,
                DirectoryVersioned.class);
        DirectoryTxnLocal parentDir = txn.get(parentId, false, DirectoryVersioned.class);
        parentDir.removeEntry(fname, getFileClass(fname));
    }

    @Override
    public DirectoryTxnLocal createDirectory(TxnHandle txn, String dname, String path) throws WrongTypeException,
            NoSuchObjectException, VersionNotFoundException, NetworkException, ClassNotFoundException {
        String pathToParent = DirectoryTxnLocal.getPathToParent(new CRDTIdentifier(table, path));
        String parent = DirectoryTxnLocal.getEntryName(new CRDTIdentifier(table, path));

        CRDTIdentifier parentId = DirectoryTxnLocal.getCRDTIdentifier(table, pathToParent, parent,
                DirectoryVersioned.class);
        DirectoryTxnLocal parentDir = txn.get(parentId, false, DirectoryVersioned.class);
        CRDTIdentifier dirId = parentDir.createNewEntry(dname, DirectoryVersioned.class);
        DirectoryTxnLocal dir = txn.get(dirId, true, DirectoryVersioned.class);
        return dir;
    }

    @Override
    public void removeDirectory(TxnHandle txn, String dname, String path) throws WrongTypeException,
            NoSuchObjectException, VersionNotFoundException, NetworkException, ClassNotFoundException {
        String pathToParent = DirectoryTxnLocal.getPathToParent(new CRDTIdentifier(table, path));
        String parent = DirectoryTxnLocal.getEntryName(new CRDTIdentifier(table, path));

        CRDTIdentifier parentId = DirectoryTxnLocal.getCRDTIdentifier(table, pathToParent, parent,
                DirectoryVersioned.class);
        DirectoryTxnLocal parentDir = txn.get(parentId, false, DirectoryVersioned.class);
        parentDir.removeEntry(dname, DirectoryVersioned.class);
    }

    @Override
    public void copyFile(TxnHandle txn, String fname, String oldpath, String newpath) throws WrongTypeException,
            NoSuchObjectException, VersionNotFoundException, NetworkException {
        CRDTIdentifier fileId = DirectoryTxnLocal.getCRDTIdentifier(table, oldpath, fname, getFileClass(fname));
        TxnGetterSetter<Blob> fileContent = (TxnGetterSetter<Blob>) txn.get(fileId, false, getFileClass(fname));

        CRDTIdentifier newFileId = DirectoryTxnLocal.getCRDTIdentifier(table, newpath, fname, getFileClass(fname));
        TxnGetterSetter<Blob> fileBasic = (TxnGetterSetter<Blob>) txn.get(newFileId, true, getFileClass(fname));
        fileBasic.set(fileContent.getValue());
    }

    @Override
    public DirectoryTxnLocal getDirectory(TxnHandle txn, String path) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        CRDTIdentifier dirId = new CRDTIdentifier(table, DirectoryTxnLocal.getDirEntry(path, DirectoryVersioned.class));
        return (DirectoryTxnLocal) txn.get(dirId, false, DirectoryVersioned.class);
    }

    @Override
    public boolean isDirectory(TxnHandle txn, String dname, String path) throws WrongTypeException,
            VersionNotFoundException, NetworkException {
        File fdummy = new File(path);
        CRDTIdentifier parentId;
        if (fdummy.getParent().equals("/")) {
            parentId = DirectoryTxnLocal.createRootId(table, fdummy.getName(), DirectoryVersioned.class);
        } else {
            parentId = DirectoryTxnLocal.getCRDTIdentifier(table, fdummy.getParent(), fdummy.getName(),
                    DirectoryVersioned.class);
        }
        DirectoryTxnLocal parent;
        try {
            parent = txn.get(parentId, false, DirectoryVersioned.class);
        } catch (NoSuchObjectException e) {
            return false;
        }
        Collection<Pair<String, Class<?>>> entries = parent.getValue();
        return entries.contains(new Pair<String, Class<?>>(dname, DirectoryVersioned.class));
    }

    @Override
    public boolean isFile(TxnHandle txn, String fname, String path) throws WrongTypeException,
            VersionNotFoundException, NetworkException {
        File fdummy = new File(path);
        CRDTIdentifier parentId;
        if (fdummy.getParent().equals("/")) {
            parentId = DirectoryTxnLocal.createRootId(table, fdummy.getName(), DirectoryVersioned.class);
        } else {
            parentId = DirectoryTxnLocal.getCRDTIdentifier(table, fdummy.getParent(), fdummy.getName(),
                    DirectoryVersioned.class);
        }
        DirectoryTxnLocal parent;
        try {
            parent = txn.get(parentId, false, DirectoryVersioned.class);
        } catch (NoSuchObjectException e) {
            return false;
        }
        Collection<Pair<String, Class<?>>> entries = parent.getValue();
        return entries.contains(new Pair<String, Class<?>>(fname, getFileClass(fname)));
    }
}
