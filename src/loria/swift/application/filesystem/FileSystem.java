/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package loria.swift.application.filesystem;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import loria.swift.crdt.MaxCausalityClockTxnLocal;
import java.util.logging.Logger;
import loria.swift.crdt.MaxCausalityClockVersionned;
import swift.application.social.User;
import swift.clocks.CausalityClock;
import swift.crdt.CRDTIdentifier;
import swift.crdt.RegisterTxnLocal;
import swift.crdt.RegisterVersioned;
import swift.crdt.SetStrings;
import swift.crdt.SetTxnLocalString;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.ObjectUpdatesListener;
import swift.crdt.interfaces.Swift;
import swift.crdt.interfaces.TxnHandle;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.SwiftException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

/**
 *
 * @author urso
 */
public class FileSystem {
    private static Logger logger = Logger.getLogger("swift.filesystem");
            
    private Swift server;
    private final IsolationLevel isolationLevel;
    private final CachePolicy cachePolicy;
    private final ObjectUpdatesListener updatesSubscriber;
    private final boolean asyncCommit;

    // Class of maximun Causality Clock CRDT
    private final Class maxCCClass;
    // Class of file content CRDT
    private final Class fileContentClass;
    
    public FileSystem(Swift clientServer, IsolationLevel isolationLevel, CachePolicy cachePolicy,
            boolean subscribeUpdates, boolean asyncCommit, 
            Class maxCCClass, Class<? extends FileContent> fileContentClass) {
        server = clientServer;
        this.isolationLevel = isolationLevel;
        this.cachePolicy = cachePolicy;
        this.updatesSubscriber = subscribeUpdates ? TxnHandle.UPDATES_SUBSCRIBER : null;
        this.asyncCommit = asyncCommit;
        this.maxCCClass = maxCCClass;
        this.fileContentClass = fileContentClass;
    }        
    
    void updateFile(String filePath, String newValue) {
        logger.log(Level.INFO, "Update file {0} with content \n{1}", new Object[]{filePath, newValue});
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(isolationLevel, cachePolicy, false);
            File file = getFile(txn, filePath);
            CausalityClock wipeClock = getWipeClock(txn, file.maxCCId); 
            FileContent content = getContent(txn, filePath, wipeClock); 
            content.update(newValue);
            commitTxn(txn);
        } catch (SwiftException e) {
            logger.warning(e.getMessage());
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }
    }

    /*
     * Use the word tree to generate file system tree.
     */
    private LocalFolder getRoot(TxnHandle txn) 
            throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
        Map<String, FileSystemObject> root = new HashMap();
        Map<String, LocalFolder> folderMap = new HashMap();
        folderMap.put("/", new LocalFolder(""));
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
    }

    /*
     * Place the file system object under the correct father (creates it if it doesn't exist). 
     * Resolve naming conflict folder/file during creation.
     */
    private void connect(TxnHandle txn, String path, Map<String, LocalFolder> folderMap, FileSystemObject node) 
            throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
        String fatherPath = path.substring(0, path.lastIndexOf('/'));
        LocalFolder father = folderMap.get(fatherPath);
        if (father == null) {
            father = new LocalFolder(fatherPath.substring(fatherPath.lastIndexOf('/')+1));
            folderMap.put(fatherPath, father);
            connect(txn, fatherPath, folderMap, father);
        }
        resolveNameConflict(father, node);
    }
    
    /*
     * Resolve naming conflict folder/file before creation of a node.
     * Map the conflicting file in the folder with a different name (prefix : "~~~"). 
     */
    private void resolveNameConflict(LocalFolder father, FileSystemObject node) {
        if (father.content.containsKey(node.getName())) {
            if ("file".equals(node.getType())) { // folder was here 
               father.content.put("~~~" + node.getName(), node);
            } else { // file was here 
               FileSystemObject file = father.content.remove(node.getName());
               father.content.put("~~~" + file.getName(), file);
               father.content.put(node.getName(), node);
            }
        } else { // No conflict
            father.content.put(node.getName(), node);
        }
    }
    
    private File getFile(TxnHandle txn, String filePath) 
            throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException  {
        return ((RegisterTxnLocal<File>) txn.get(NamingScheme.forFile(filePath), false,
                    RegisterVersioned.class)).getValue();
    }    
    
    private Folder getFolder(TxnHandle txn, String folderPath) 
            throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException  {
        return ((RegisterTxnLocal<Folder>) txn.get(NamingScheme.forFolder(folderPath), false,
                    RegisterVersioned.class)).getValue();
    }
    
    private CausalityClock getWipeClock(TxnHandle txn, CRDTIdentifier maxCCId) 
            throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException  {
        return ((MaxCausalityClockTxnLocal) txn.get(maxCCId, false, maxCCClass)).getValue();
    }

    private FileContent getContent(TxnHandle txn, String filePath, CausalityClock wipeClock) 
            throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
        return ((FileContent) txn.get(NamingScheme.forContent(filePath, wipeClock), false, fileContentClass));
    }
    
    
    private void commitTxn(final TxnHandle txn) {
        if (asyncCommit) {
            txn.commitAsync(null);
        } else {
            txn.commit();
        }
    }






}
