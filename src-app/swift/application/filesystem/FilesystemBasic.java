/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2012 University of Kaiserslautern
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package swift.application.filesystem;

import java.io.File;
import java.util.Collection;

import swift.crdt.DirectoryCRDT;
import swift.crdt.LWWRegisterCRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnGetterSetter;
import swift.crdt.core.TxnHandle;
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

    public FilesystemBasic(TxnHandle txn, String root, String table) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        this.table = table;
        this.root = DirectoryCRDT.createRootId(table, root, DirectoryCRDT.class);
        txn.get(this.root, true, DirectoryCRDT.class);
    }

    // FIXME
    private static Class getFileClass(String name) {
        if (name.endsWith(".txt")) {
            // return LogootVersioned.class;
            return null; // LogootVersioned.class;
        }
        return LWWRegisterCRDT.class;
    }

    @Override
    public IFile createFile(TxnHandle txn, String fname, String path) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException, ClassNotFoundException {

        String pathToParent = DirectoryCRDT.getPathToParent(new CRDTIdentifier(table, path));
        String parent = DirectoryCRDT.getEntryName(new CRDTIdentifier(table, path));

        CRDTIdentifier parentId = DirectoryCRDT.getCRDTIdentifier(table, pathToParent, parent, DirectoryCRDT.class);
        DirectoryCRDT parentDir = txn.get(parentId, false, DirectoryCRDT.class);
        CRDTIdentifier fileId = parentDir.createNewEntry(fname, getFileClass(fname));
        TxnGetterSetter<Blob> fileContent = (TxnGetterSetter<Blob>) txn.get(fileId, true, getFileClass(fname));
        Blob initialFileContent = new Blob();
        fileContent.set(initialFileContent);
        return new FilePaged(initialFileContent);
    }

    @Override
    public IFile readFile(TxnHandle txn, String fname, String path) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {

        CRDTIdentifier fileId = DirectoryCRDT.getCRDTIdentifier(table, path, fname, getFileClass(fname));
        TxnGetterSetter<Blob> fileContent = (TxnGetterSetter<Blob>) txn.get(fileId, false, getFileClass(fname));
        return new FilePaged(fileContent.getValue());
    }

    @Override
    public void updateFile(TxnHandle txn, String fname, String path, IFile f) throws WrongTypeException,
            NoSuchObjectException, VersionNotFoundException, NetworkException {
        CRDTIdentifier fileId = DirectoryCRDT.getCRDTIdentifier(table, path, fname, getFileClass(fname));
        TxnGetterSetter<Blob> content = (TxnGetterSetter<Blob>) txn.get(fileId, false, getFileClass(fname));
        content.set(new Blob(f.getBytes()));
    }

    @Override
    public void removeFile(TxnHandle txn, String fname, String path) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException, ClassNotFoundException {
        String pathToParent = DirectoryCRDT.getPathToParent(new CRDTIdentifier(table, path));
        String parent = DirectoryCRDT.getEntryName(new CRDTIdentifier(table, path));

        CRDTIdentifier parentId = DirectoryCRDT.getCRDTIdentifier(table, pathToParent, parent, DirectoryCRDT.class);
        DirectoryCRDT parentDir = txn.get(parentId, false, DirectoryCRDT.class);
        parentDir.removeEntry(fname, getFileClass(fname));
    }

    @Override
    public DirectoryCRDT createDirectory(TxnHandle txn, String dname, String path) throws WrongTypeException,
            NoSuchObjectException, VersionNotFoundException, NetworkException, ClassNotFoundException {
        String pathToParent = DirectoryCRDT.getPathToParent(new CRDTIdentifier(table, path));
        String parent = DirectoryCRDT.getEntryName(new CRDTIdentifier(table, path));

        CRDTIdentifier parentId = DirectoryCRDT.getCRDTIdentifier(table, pathToParent, parent, DirectoryCRDT.class);
        DirectoryCRDT parentDir = txn.get(parentId, false, DirectoryCRDT.class);
        CRDTIdentifier dirId = parentDir.createNewEntry(dname, DirectoryCRDT.class);
        DirectoryCRDT dir = txn.get(dirId, true, DirectoryCRDT.class);
        return dir;
    }

    @Override
    public void removeDirectory(TxnHandle txn, String dname, String path) throws WrongTypeException,
            NoSuchObjectException, VersionNotFoundException, NetworkException, ClassNotFoundException {
        String pathToParent = DirectoryCRDT.getPathToParent(new CRDTIdentifier(table, path));
        String parent = DirectoryCRDT.getEntryName(new CRDTIdentifier(table, path));

        CRDTIdentifier parentId = DirectoryCRDT.getCRDTIdentifier(table, pathToParent, parent, DirectoryCRDT.class);
        DirectoryCRDT parentDir = txn.get(parentId, false, DirectoryCRDT.class);
        parentDir.removeEntry(dname, DirectoryCRDT.class);
    }

    @Override
    public void copyFile(TxnHandle txn, String fname, String oldpath, String newpath) throws WrongTypeException,
            NoSuchObjectException, VersionNotFoundException, NetworkException, ClassNotFoundException {
        CRDTIdentifier fileId = DirectoryCRDT.getCRDTIdentifier(table, oldpath, fname, getFileClass(fname));
        TxnGetterSetter<Blob> fileContent = (TxnGetterSetter<Blob>) txn.get(fileId, false, getFileClass(fname));

        createFile(txn, fname, newpath);
        CRDTIdentifier newFileId = DirectoryCRDT.getCRDTIdentifier(table, newpath, fname, getFileClass(fname));
        TxnGetterSetter<Blob> fileBasic = (TxnGetterSetter<Blob>) txn.get(newFileId, true, getFileClass(fname));
        fileBasic.set(fileContent.getValue());
    }

    @Override
    public DirectoryCRDT getDirectory(TxnHandle txn, String path) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        CRDTIdentifier dirId = new CRDTIdentifier(table, DirectoryCRDT.getDirEntry(path, DirectoryCRDT.class));
        return (DirectoryCRDT) txn.get(dirId, false, DirectoryCRDT.class);
    }

    @Override
    public boolean isDirectory(TxnHandle txn, String dname, String path) throws WrongTypeException,
            VersionNotFoundException, NetworkException {
        return getContentOfParentDirectory(txn, path, dname, DirectoryCRDT.class);
    }

    @Override
    public boolean isFile(TxnHandle txn, String fname, String path) throws WrongTypeException,
            VersionNotFoundException, NetworkException {
        return getContentOfParentDirectory(txn, path, fname, getFileClass(fname));
    }

    private boolean getContentOfParentDirectory(TxnHandle txn, String path, String name, Class<?> type)
            throws WrongTypeException, VersionNotFoundException, NetworkException {
        File fdummy = new File(path);
        CRDTIdentifier parentId;
        if ("/".equals(fdummy.getParent())) {
            parentId = DirectoryCRDT.createRootId(table, fdummy.getName(), DirectoryCRDT.class);
        } else {
            parentId = DirectoryCRDT
                    .getCRDTIdentifier(table, fdummy.getParent(), fdummy.getName(), DirectoryCRDT.class);
        }
        try {
            DirectoryCRDT parent = txn.get(parentId, false, DirectoryCRDT.class);
            Collection<Pair<String, Class<?>>> entries = parent.getValue();
            return entries.contains(new Pair<String, Class<?>>(name, type));
        } catch (NoSuchObjectException e) {
            return false;
        }
    }
}
