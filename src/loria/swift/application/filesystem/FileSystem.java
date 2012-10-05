/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package loria.swift.application.filesystem;

import loria.swift.crdt.MaxCausalityClockTxnLocal;
import java.util.logging.Logger;
import loria.swift.crdt.MaxCausalityClockVersionned;
import swift.application.social.User;
import swift.clocks.CausalityClock;
import swift.crdt.CRDTIdentifier;
import swift.crdt.RegisterTxnLocal;
import swift.crdt.RegisterVersioned;
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
        logger.info("Update file " + filePath + " with content \n" + newValue);
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

    private File getFile(TxnHandle txn, String filePath) 
            throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException  {
        return ((RegisterTxnLocal<File>) txn.get(NamingScheme.forFile(filePath), false,
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
