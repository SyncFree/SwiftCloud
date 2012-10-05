package swift.application.filesystem;

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

    public FilesystemBasic(TxnHandle txn, String root, String table) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        this.table = table;
        this.root = DirectoryTxnLocal.createRootId(table, root, DirectoryVersioned.class);
        txn.get(this.root, true, DirectoryVersioned.class);
    }

    @Override
    public File createFile(TxnHandle txn, String fname, String path) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {

        String pathToParent = DirectoryTxnLocal.getPathToParent(new CRDTIdentifier(table, path));
        String parent = DirectoryTxnLocal.getEntryName(new CRDTIdentifier(table, path));

        CRDTIdentifier parentId = DirectoryTxnLocal.getCRDTIdentifier(table, pathToParent, parent,
                DirectoryVersioned.class);
        DirectoryTxnLocal parentDir = txn.get(parentId, false, DirectoryVersioned.class);
        CRDTIdentifier fileId = parentDir.createNewEntry(fname, RegisterVersioned.class);
        RegisterTxnLocal<StringCopyable> fileContent = (RegisterTxnLocal<StringCopyable>) txn.get(fileId, true,
                RegisterVersioned.class);
        String initialFileContent = "";
        fileContent.set(new StringCopyable(initialFileContent));
        return new FileBasic(initialFileContent);
    }

    @Override
    public File readFile(TxnHandle txn, String fname, String path) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {

        CRDTIdentifier fileId = DirectoryTxnLocal.getCRDTIdentifier(table, path, fname, RegisterVersioned.class);
        RegisterTxnLocal<StringCopyable> fileContent = (RegisterTxnLocal<StringCopyable>) txn.get(fileId, true,
                RegisterVersioned.class);
        return new FileBasic(fileContent.getValue().getString());
    }

    @Override
    public void updateFile(TxnHandle txn, String fname, String path, File f) throws WrongTypeException,
            NoSuchObjectException, VersionNotFoundException, NetworkException {
        CRDTIdentifier fileId = DirectoryTxnLocal.getCRDTIdentifier(table, path, fname, RegisterVersioned.class);
        RegisterTxnLocal<StringCopyable> fileBasic = (RegisterTxnLocal<StringCopyable>) txn.get(fileId, false,
                RegisterVersioned.class);
        fileBasic.set(new StringCopyable(f.getContent()));
    }

    @Override
    public void removeFile(TxnHandle txn, String fname, String path) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException, ClassNotFoundException {
        String pathToParent = DirectoryTxnLocal.getPathToParent(new CRDTIdentifier(table, path));
        String parent = DirectoryTxnLocal.getEntryName(new CRDTIdentifier(table, path));

        CRDTIdentifier parentId = DirectoryTxnLocal.getCRDTIdentifier(table, pathToParent, parent,
                DirectoryVersioned.class);
        DirectoryTxnLocal parentDir = txn.get(parentId, false, DirectoryVersioned.class);
        parentDir.removeEntry(fname, RegisterVersioned.class);
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

}
