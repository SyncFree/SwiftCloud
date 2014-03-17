/*****************************************************************************
 * Copyright 2011-2014 INRIA
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
package swift.crdt;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.core.BaseCRDT;
import swift.crdt.core.CRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;
import swift.utils.Pair;

/**
 * Directory CRDT.
 * 
 * @author annettebieniusa,mzawirsk
 */

/*
 * Some information on dynamic type checking in Java for compliance of
 * identifiers and CRDT types:
 * 
 * To obtain the fully qualified name of a class: val.getClass().getName() or
 * V.getName()
 * 
 * To reconstruct the class and type-safe casts: w = Class.forName(FQN);
 * assert(w == v); v.cast(object)
 */
public class DirectoryCRDT extends BaseCRDT<DirectoryCRDT> {
    public static String getClassName(CRDTIdentifier id) {
        return id.getKey().split(":")[1];
    }

    public static String getFullPath(CRDTIdentifier id) {
        return id.getKey().split(":")[0];
    }

    public static String getEntryName(CRDTIdentifier id) {
        String path = getFullPath(id);
        String[] splitted = path.split("/");
        return splitted[splitted.length - 1];
    }

    public static String getPathToParent(CRDTIdentifier path) {
        String[] parentDirs = getFullPath(path).split("/");
        String parent = "";
        for (int i = 1; i < parentDirs.length - 1; i++) {
            parent = parent.concat("/");
            parent = parent.concat(parentDirs[i]);
        }
        return parent;
    }

    public static <V extends CRDT<V>> String getDirEntry(String name, Class<V> c) {
        return name + ":" + c.getName();
    }

    public static <V extends CRDT<V>> CRDTIdentifier getCRDTIdentifier(String table, String fullDirName, String name,
            Class<V> c) {
        String prefix = "/".equals(fullDirName) ? "" : fullDirName + "/";
        return new CRDTIdentifier(table, prefix + getDirEntry(name, c));
    }

    public static <V extends CRDT<V>> CRDTIdentifier createRootId(String table, String key, Class<V> c) {
        CRDTIdentifier newCRDTId = getCRDTIdentifier(table, "", key, c);
        return newCRDTId;

    }

    protected Map<CRDTIdentifier, Set<TripleTimestamp>> dir;

    // Kryo
    public DirectoryCRDT() {
    }

    public DirectoryCRDT(CRDTIdentifier id) {
        super(id);
        this.dir = new HashMap<CRDTIdentifier, Set<TripleTimestamp>>();
    }

    private DirectoryCRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock,
            Map<CRDTIdentifier, Set<TripleTimestamp>> dir) {
        super(id, txn, clock);
        this.dir = dir;
    }

    /**
     * Table under which all entries for this directory (and all its
     * subdirectories) are stored.
     */
    public String getDirTable() {
        return id.getTable();
    }

    public <V extends CRDT<V>> V get(String key, Class<V> c) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        CRDTIdentifier entry = getCRDTIdentifier(getDirTable(), getFullPath(this.id), key, c);
        if (dir.keySet().contains(entry)) {
            return this.getTxnHandle().get(entry, false, c, null);
        } else {
            return null;
        }
    }

    public <V extends CRDT<V>> boolean contains(String key, Class<V> c) {
        return dir.containsKey(getCRDTIdentifier(getDirTable(), getFullPath(this.id), key, c));
    }

    @Override
    public Collection<Pair<String, Class<?>>> getValue() {
        Collection<Pair<String, Class<?>>> dirEntries = new HashSet<Pair<String, Class<?>>>();
        for (CRDTIdentifier id : dir.keySet()) {
            try {
                String name = getEntryName(id);
                String clss = getClassName(id);
                Pair<String, Class<?>> entry = new Pair<String, Class<?>>(name, Class.forName(clss));
                dirEntries.add(entry);
            } catch (ClassNotFoundException e) {
                System.err.println("This class is unkown :" + id.getKey());
                e.printStackTrace();
            }
        }
        return dirEntries;
    }

    public <V extends CRDT<V>> CRDTIdentifier createNewEntry(String key, Class<V> c) throws WrongTypeException,
            NoSuchObjectException, VersionNotFoundException, NetworkException {
        // implemented as remove followed by add
        TripleTimestamp ts = nextTimestamp();
        CRDTIdentifier newCRDTId = getCRDTIdentifier(getDirTable(), getFullPath(this.id), key, c);
        Set<TripleTimestamp> overwrittenInstances = AddWinsUtils.add(dir, newCRDTId, ts);
        this.getTxnHandle().get(newCRDTId, true, c);
        registerLocalOperation(new DirectoryCreateUpdate(newCRDTId, ts, overwrittenInstances));

        // Reconstruct the path to the root for implementing the
        // add-/update-wins semantics
        String name = getEntryName(this.id);
        String path = getPathToParent(this.id);
        if (!"".equals(path)) {
            CRDTIdentifier parentId = new CRDTIdentifier(getDirTable(), getDirEntry(path, DirectoryCRDT.class));
            DirectoryCRDT parent = this.getTxnHandle().get(parentId, false, DirectoryCRDT.class);
            parent.createNewEntry(name, DirectoryCRDT.class);
        }
        return newCRDTId;
    }

    public <V extends CRDT<V>> void removeEntry(String key, Class<V> c) throws WrongTypeException,
            NoSuchObjectException, VersionNotFoundException, NetworkException, ClassNotFoundException {
        CRDTIdentifier id = getCRDTIdentifier(getDirTable(), getFullPath(this.id), key, c);
        Set<TripleTimestamp> toBeRemoved = AddWinsUtils.remove(dir, id);
        if (toBeRemoved != null) {
            registerLocalOperation(new DirectoryRemoveUpdate(getCRDTIdentifier(getDirTable(), getFullPath(this.id),
                    key, c), toBeRemoved));
        }
        if (c.equals(DirectoryCRDT.class)) {
            try {
                DirectoryCRDT child = (DirectoryCRDT) this.getTxnHandle().get(id, false, c);
                child.deleteDirectoryRecursively(c);
            } catch (NoSuchObjectException x) {
                // TODO: ignore?
            }
        }
    }

    private <V extends CRDT<V>> void deleteDirectoryRecursively(Class<V> c) throws ClassNotFoundException,
            WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
        Set<CRDTIdentifier> currentEntries = new HashSet<CRDTIdentifier>(this.dir.keySet());
        for (CRDTIdentifier e : currentEntries) {
            removeEntry(getEntryName(e), (Class<V>) Class.forName(getClassName(e)));
        }
    }

    protected void applyCreate(CRDTIdentifier entry, TripleTimestamp ts, Set<TripleTimestamp> overwrittenTimestamps) {
        AddWinsUtils.applyAdd(dir, entry, ts, overwrittenTimestamps);
    }

    protected void applyRemove(CRDTIdentifier key, Set<TripleTimestamp> toBeRemoved) {
        AddWinsUtils.applyRemove(dir, key, toBeRemoved);
    }

    @Override
    public DirectoryCRDT copy() {
        final Map<CRDTIdentifier, Set<TripleTimestamp>> newDir = new HashMap<CRDTIdentifier, Set<TripleTimestamp>>();
        AddWinsUtils.deepCopy(dir, newDir);
        return new DirectoryCRDT(id, txn, clock, newDir);
    }
}
