/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package loria.swift.application.filesystem;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import swift.crdt.CRDTIdentifier;
import swift.crdt.SetIds;
import swift.crdt.SetStrings;
import swift.crdt.SetTxnLocalId;
import swift.crdt.SetTxnLocalString;
import swift.crdt.interfaces.TxnHandle;

/**
 * Local view of a folder. Contains a map of file system objects.
 *
 * @author urso
 * @author Stephane Martin <stephane.martin@loria.fr>
 */
public class FolderSet extends Folder {

   
    SetTxnLocalId localSet;
    Map<String, FileSystemObject> content;
    List <FileSystemObject> files;
   

    public FolderSet(TxnHandle txn, String pwd) {
        super(txn, pwd);
        
    }

    private void load() {
        if (localSet == null) {

            try {
                localSet = (SetTxnLocalId) txn.get(NamingScheme.forFolder(this.getPwd()), true, SetIds.class);
                //content = new HashMap<String, FileSystemObject>();
            } catch (Exception ex) {
                Logger.getLogger(this.getClass().getName()).severe(ex.toString());

            }
        }
    }

    @Override
    public List<FileSystemObject> getList() {
        load();
        if (files==null){
            files=new LinkedList();
            for (CRDTIdentifier ids: localSet.getValue()){
                if (ids.getTable().equals(NamingScheme.FOLDERS)){
                    files.add(new FolderSet(txn,ids.getKey()));
                }else if (ids.getTable().equals(NamingScheme.FILES)){
                    files.add(new File(txn,ids.getKey()));
                }
            }
        }
        return files;
    }

   /* public Map<String, FileSystemObject> getContent() {
        return content;
    }*/

    
    
    @Override
    public void deleteFile(String name){
        throw new UnsupportedOperationException("not yet");
    }

    @Override
    public String getType() {
        return "folder";
    }

    @Override
    public Folder getFolder(String pwd) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public File getFile(String pwd,boolean create) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public File createNewFile(String name) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    

    @Override
    public void uptodate(TxnHandle txn) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isExisting() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
