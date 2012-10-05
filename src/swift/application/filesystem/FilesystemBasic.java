package swift.application.filesystem;

import loria.swift.application.filesystem.mapper.FileContent;
import loria.swift.application.filesystem.mapper.RegisterFileContent;
import swift.crdt.CRDTIdentifier;
import swift.crdt.DirectoryTxnLocal;
import swift.crdt.DirectoryVersioned;
import swift.crdt.RegisterTxnLocal;
import swift.crdt.RegisterVersioned;
import swift.crdt.interfaces.TxnHandle;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

public class FilesystemBasic implements Filesystem {
    // table in DHT that holds all entries for this filesystem
    private String table;
    // root directory
    private CRDTIdentifier root;
    private final Class fileContentClass;
    
    public FilesystemBasic(TxnHandle txn, String root, String table, Class fileContentClass) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        this.table = table;
        this.root = DirectoryTxnLocal.createRootId(table, root, DirectoryVersioned.class);
        txn.get(this.root, true, DirectoryVersioned.class);
        this.fileContentClass = fileContentClass;
    }
    
    public FilesystemBasic(TxnHandle txn, String root, String table) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        this(txn, root, table, RegisterFileContent.class);
    }

    @Override
    public File createFile(TxnHandle txn, String fname, String path) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {

        String pathToParent = DirectoryTxnLocal.getPathToParent(new CRDTIdentifier(table, path));
        String parent = DirectoryTxnLocal.getEntryName(new CRDTIdentifier(table, path));

        CRDTIdentifier parentId = DirectoryTxnLocal.getCRDTIdentifier(table, pathToParent, parent,
                DirectoryVersioned.class);
        DirectoryTxnLocal parentDir = txn.get(parentId, false, DirectoryVersioned.class);
        CRDTIdentifier fileId = parentDir.createNewEntry(fname, fileContentClass);
        FileContent fileContent = (FileContent) txn.get(fileId, true, fileContentClass);
        String initialFileContent = "";
        fileContent.set(initialFileContent);
        return new FileBasic(initialFileContent);
    }

    @Override
    public File readFile(TxnHandle txn, String fname, String path) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {

        CRDTIdentifier fileId = DirectoryTxnLocal.getCRDTIdentifier(table, path, fname, fileContentClass);
        FileContent fileContent = (FileContent) txn.get(fileId, true, fileContentClass);
        return new FileBasic(fileContent.getText());
    }

    @Override
    public void updateFile(TxnHandle txn, String fname, String path, File f) throws WrongTypeException,
            NoSuchObjectException, VersionNotFoundException, NetworkException {
        CRDTIdentifier fileId = DirectoryTxnLocal.getCRDTIdentifier(table, path, fname, fileContentClass);
        FileContent fileBasic = (FileContent) txn.get(fileId, false, fileContentClass);
        fileBasic.set(f.getContent());
    }

    @Override
    public void removeFile(TxnHandle txn, String fname, String path) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException, ClassNotFoundException {
        String pathToParent = DirectoryTxnLocal.getPathToParent(new CRDTIdentifier(table, path));
        String parent = DirectoryTxnLocal.getEntryName(new CRDTIdentifier(table, path));

        CRDTIdentifier parentId = DirectoryTxnLocal.getCRDTIdentifier(table, pathToParent, parent,
                DirectoryVersioned.class);
        DirectoryTxnLocal parentDir = txn.get(parentId, false, DirectoryVersioned.class);
        parentDir.removeEntry(fname, fileContentClass);
    }

    @Override
    public DirectoryTxnLocal createDirectory(TxnHandle txn, String dname, String path) throws WrongTypeException,
            NoSuchObjectException, VersionNotFoundException, NetworkException {
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
        CRDTIdentifier fileId = DirectoryTxnLocal.getCRDTIdentifier(table, oldpath, fname, fileContentClass);
        FileContent fileContent = (FileContent) txn.get(fileId, true, fileContentClass);

        CRDTIdentifier newFileId = DirectoryTxnLocal.getCRDTIdentifier(table, newpath, fname, fileContentClass);
        FileContent fileBasic = (FileContent) txn.get(newFileId, true, fileContentClass);
        fileBasic.set(fileContent.getText());
    }

    @Override
    public DirectoryTxnLocal getDirectory(TxnHandle txn, String path) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        CRDTIdentifier dirId = new CRDTIdentifier(table, DirectoryTxnLocal.getDirEntry(path, DirectoryVersioned.class));
        return (DirectoryTxnLocal) txn.get(dirId, false, DirectoryVersioned.class);
    }

}
