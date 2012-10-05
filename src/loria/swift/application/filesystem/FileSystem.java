/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package loria.swift.application.filesystem;

import java.util.ConcurrentModificationException;
import java.util.logging.Level;
import java.util.logging.Logger;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.Swift;
import swift.crdt.interfaces.TxnHandle;

/**
 *
 * @author urso
 * @author Stephane Martin <stephane.martin@loria.fr>
 */
public class FileSystem {

    private Logger logger = Logger.getLogger(this.getClass().getName());
    /* private Swift server;
     private final IsolationLevel isolationLevel;
     private final CachePolicy cachePolicy;
     private final ObjectUpdatesListener updatesSubscriber;
     private final boolean asyncCommit;
     private CausalityClock clockReference;*/
    private Folder root;

    enum FolderStrategy {

        WordTree, SetByFolder
    };
    private FolderStrategy folderStrategy = FolderStrategy.WordTree;

    public FileSystem(Swift clientServer, IsolationLevel isolationLevel, CachePolicy cachePolicy,
            boolean subscribeUpdates, boolean asyncCommit) {
        /* server = clientServer;
         this.isolationLevel = isolationLevel;
         this.cachePolicy = cachePolicy;
         this.updatesSubscriber = subscribeUpdates ? TxnHandle.UPDATES_SUBSCRIBER : null;
         this.asyncCommit = asyncCommit;*/
    }

    public FileSystem() {
    }

    public String getFileValue(String filePath, TxnHandle txn) {
        File f = this.getRoot(txn).getFile(filePath, false);
        return (f == null) ? null : f.getContent();
    }

    public void updateFile(String filePath, String newValue, boolean create, TxnHandle txn) {

        logger.log(Level.INFO, "Update file {0} with content \n{1}", new Object[]{filePath, newValue});

        //try {
        //txn = server.beginTxn(isolationLevel, cachePolicy, false);
        File file = this.getRoot(txn).getFile(filePath, create);
        if (file == null) {
            throw new ConcurrentModificationException("File is not here");
        }
        file.update(newValue);
        //commitTxn(txn);
        /*} catch (SwiftException e) {
         logger.warning(e.getMessage());
         } finally {
         if (txn != null && !txn.getStatus().isTerminated()) {
         txn.rollback();
         }
         }*/
    }

    private Folder getRootFolder(TxnHandle txn) {
        Folder ret = null;
        switch (this.folderStrategy) {
            case WordTree:
                ret = new FolderOneWordTree(txn, "/");
                break;
            case SetByFolder:
                ret = new FolderSet(txn, "/");
                break;
        }

        return ret;
    }

    public Folder getRoot(TxnHandle txn) {
        if (root == null) {
            root = getRootFolder(txn);
        } else {
            root.uptodate(txn);
        }
        return root;
    }

    public Folder getFolder(TxnHandle txn, String name) {
        return this.getRoot(txn).getFolder(name);
    }

    /*public initFolder(String name){
        
     }*/
    /*
     * Use the word tree to generate file system tree.
     */
    /*private FolderSet getRoot(TxnHandle txn) 
     throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
     Map<String, FileSystemObject> root = new HashMap();
     Map<String, FolderSet> folderMap = new HashMap();
     folderMap.put("/", new FolderSet(""));
     Set<String> words = ((SetTxnLocalString) txn.get(NamingScheme.forTree(), false, SetStrings.class)).getValue();
     List<Set<String>> buckets = new ArraySkipList<Set<String>>();
        
     for (String w : words) {
     String[] d = w.split("/");
     int size = d.length-2;
     Set<String> b = buckets.get(size);
     if (b == null) {
     b = new HashSet<String>();
     buckets.set(size, b);
     }
     b.add(w);
     }
        
     for (Set<String> b : buckets) {
     for (String path : b) {
     File file = getFile(txn, path);
     connect(txn, path, folderMap, file);
     }
     }
     return folderMap.get("/");
     }*/

    /*
     * Place the file system object under the correct father.
     * Reappear policy : creates the father if it doesn't exist. 
     * Resolve naming conflict folder/file during creation.
     */
    /*  private void connect(TxnHandle txn, String path, Map<String, FolderSet> folderMap, FileSystemObject node)
     throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
     String fatherPath = path.substring(0, path.lastIndexOf('/'));
     FolderSet father = folderMap.get(fatherPath);
     if (father == null) {
     father = new FolderSet(fatherPath.substring(fatherPath.lastIndexOf('/') + 1));
     folderMap.put(fatherPath, father);
     connect(txn, fatherPath, folderMap, father);
     }
     resolveNameConflict(father, node);
     }*/

    /*
     * Resolve naming conflict folder/file during creation of a node.
     * Map the conflicting file in the folder with a different name (prefix : "~~~"). 
     * Due to connection algorithm folder always appears after file.
     */
    /* private void resolveNameConflict(FolderSet father, FileSystemObject node) {
     if (father.content.containsKey(node.getName())) { // folder appears
     FileSystemObject file = father.content.remove(node.getName());
     father.content.put("~~~" + file.getName(), file);
     }
     father.content.put(node.getName(), node);
     }*/

    /*private File getFile(TxnHandle txn, String filePath)
     throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
     return ((RegisterTxnLocal<File>) txn.get(NamingScheme.forFile(filePath), false,
     RegisterVersioned.class)).getValue();
     }

     private FolderOld getFolder(TxnHandle txn, String folderPath)
     throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
     return ((RegisterTxnLocal<FolderOld>) txn.get(NamingScheme.forFolder(folderPath), false,
     RegisterVersioned.class)).getValue();
     }*/

    /* private void commitTxn(final TxnHandle txn) {
     if (asyncCommit) {
     txn.commitAsync(null);
     } else {
     txn.commit();
     }
     }*/
}
