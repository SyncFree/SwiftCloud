package swift.application.filesystem;

import java.io.IOException;

import swift.crdt.DirectoryTxnLocal;
import swift.crdt.interfaces.TxnHandle;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

public interface Filesystem {
    IFile createFile(TxnHandle txn, String fname, String path) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException, IOException, ClassNotFoundException;

    DirectoryTxnLocal createDirectory(TxnHandle txn, String name, String path) throws WrongTypeException,
            NoSuchObjectException, VersionNotFoundException, NetworkException, ClassNotFoundException;

    void removeDirectory(TxnHandle txn, String name, String path) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException, ClassNotFoundException;

    DirectoryTxnLocal getDirectory(TxnHandle txn, String path) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException;

    void updateFile(TxnHandle txn, String fname, String path, IFile f) throws WrongTypeException,
            NoSuchObjectException, VersionNotFoundException, NetworkException, IOException;

    void removeFile(TxnHandle txn, String fname, String path) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException, ClassNotFoundException;

    IFile readFile(TxnHandle txn, String fname, String path) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException, IOException;

    void copyFile(TxnHandle txn, String fname, String oldpath, String newpath) throws WrongTypeException,
            NoSuchObjectException, VersionNotFoundException, NetworkException, ClassNotFoundException;

    boolean isDirectory(TxnHandle txn, String dname, String path) throws WrongTypeException, VersionNotFoundException,
            NetworkException;

    boolean isFile(TxnHandle txn, String fname, String path) throws WrongTypeException, VersionNotFoundException,
            NetworkException;
}
