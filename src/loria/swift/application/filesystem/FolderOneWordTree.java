/*
 *  Replication Benchmarker
 *  https://github.com/score-team/replication-benchmarker/
 *  Copyright (C) 2012 LORIA / Inria / SCORE Team
 * 
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 * 
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 * 
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.

 */
package loria.swift.application.filesystem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import swift.crdt.SetStrings;
import swift.crdt.SetTxnLocalString;
import swift.crdt.interfaces.TxnHandle;

/**
 *
 * @author Stephane Martin <stephane.martin@loria.fr>
 */
public class FolderOneWordTree extends Folder {

    boolean onDemand = false;
    SetTxnLocalString localSet;
    //Map <String,FileSystemObject> cache;
    Set<FileSystemObject> cache;
    Map<String, FolderOneWordTree> cacheDir;
    Map<String, File> cacheFile;

    public void setLocalSet(SetTxnLocalString localSet, Map<String, FolderOneWordTree> cacheDir) {
        this.localSet = localSet;
        this.cacheDir = cacheDir;
    }

    private void load() {
        if (localSet == null) {
            cache = new HashSet<FileSystemObject>();
            cacheDir = new HashMap<String, FolderOneWordTree>();
            cacheFile = new HashMap<String, File>();
            try {
                localSet = (SetTxnLocalString) txn.get(NamingScheme.forFolder("/"), true, SetStrings.class);
            } catch (Exception ex) {
                Logger.getLogger(this.getClass().getName()).severe(ex.toString());
            }
        }
    }

    private void buildFromPath(String path) {
        String subPath = path.substring(pwd.length());
        if (!subPath.equals("/") && !subPath.equals("")) {
            int index = subPath.indexOf("/", 1);
            if (index == -1) { // is file
                File f = new File(txn, path);
                this.cache.add(f);
                this.cacheFile.put(path, f);
            } else {          // folder
                String rep = subPath.substring(0, index);
                FolderOneWordTree np = cacheDir.get(rep);
                if (np == null) {
                    np = makeObjNewRep(rep);
                }
                cache.add(np);
                if (!onDemand) {
                    np.buildFromPath(path);
                }
            }
        }
    }

    /*  private FileSystemObject getFromName(String name, boolean Dir) {
     throw new UnsupportedOperationException();
     }*/
    private void checkCache() {
        load();
        String pwd2 = this.getPwd();
        //assert(!pwd2.endsWith("/"));
        //FileSystemObject root = this;
        for (String str : localSet.getValue()) {
            if (str.startsWith(pwd2)) {
                buildFromPath(str);
            }
        }
    }

    public FolderOneWordTree(TxnHandle txn, String pwd) {
        super(txn, pwd);
    }

    @Override
    public List<FileSystemObject> getList() {
        checkCache();
        return new ArrayList(cache);
    }

    @Override
    public File getFile(String pwd, boolean create) {
        checkCache();
        pwd = this.convert2Abs(pwd);
        File f = this.cacheFile.get(pwd);
        if (f == null) {
            f = new File(txn, pwd);
            if (this.isExisting(pwd, true)) {
                cacheFile.put(pwd, f);
            } else if (!create) {
                return null;
            } else {
                cacheFile.put(pwd, f);
                //f.create();
                localSet.insert(pwd);
                //Folder fold=cacheDir.get()
            }
        }
        return f;
    }

    @Override
    public Folder getFolder(String pwd) {
        pwd = this.convert2Abs(pwd);
        checkCache();
        Folder f = this.cacheDir.get(pwd);
        if (f == null && isExisting(pwd, false)) {
            f = makeObjNewRep(pwd);
        }
        return f;
    }
    /*    @Override
     public File getFile(String pwd) {
     return getFile(pwd, false);
     }*/

    @Override
    public File createNewFile(String pwd) {
        return getFile(pwd, true);

    }

    @Override
    public void uptodate(TxnHandle txn) {
        if (!this.txn.equals(txn)) {
            this.localSet = null;
            this.txn=txn;
        }
    }

    @Override
    public void deleteFile(String relPath) {
        checkCache();
        relPath = convert2Abs(relPath);
        localSet.remove(relPath);
        File file = cacheFile.remove(relPath);
        FolderOneWordTree parent = cacheDir.get(this.getParentFromAbs(relPath));
        if (file != null) {
            file.delete();
            if (parent != null) {
                parent.cache.remove(file);
            }
        }
    }

    private boolean isExisting(String path, boolean file) {
        checkCache();
        for (String str : localSet.getValue()) {
            if ((file && str.equals(path)) || (!file && str.contains(path))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isExisting() {
        return isExisting(pwd, false);
    }

    private FolderOneWordTree makeObjNewRep(String rep) {
        FolderOneWordTree np = new FolderOneWordTree(txn, rep);
        np.setLocalSet(localSet, cacheDir);
        cacheDir.put(rep, np);
        return np;
    }

    
}
