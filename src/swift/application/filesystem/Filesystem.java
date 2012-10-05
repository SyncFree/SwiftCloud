package swift.application.filesystem;

import swift.crdt.DirectoryTxnLocal;
import swift.crdt.interfaces.TxnHandle;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

public interface Filesystem {
    File createFile(TxnHandle txn, String fname, String path) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException;

    DirectoryTxnLocal createDirectory(TxnHandle txn, String name, String path) throws WrongTypeException,
            NoSuchObjectException, VersionNotFoundException, NetworkException;

    void removeDirectory(TxnHandle txn, String name, String path) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException, ClassNotFoundException;

    DirectoryTxnLocal getDirectory(TxnHandle txn, String path) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException;

    void updateFile(TxnHandle txn, String fname, String path, File f) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException;

    void removeFile(TxnHandle txn, String fname, String path) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException, ClassNotFoundException;

    File readFile(TxnHandle txn, String fname, String path) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException;

    void copyFile(TxnHandle txn, String fname, String oldpath, String newpath) throws WrongTypeException,
            NoSuchObjectException, VersionNotFoundException, NetworkException;

}
