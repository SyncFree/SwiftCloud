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

import java.util.List;
import swift.crdt.interfaces.TxnHandle;

/**
 *
 * @author Stephane Martin <stephane.martin@loria.fr>
 */
public abstract class Folder extends FileSystemObject{

    public Folder(TxnHandle txn, String pwd) {
        super(txn, pwd);
    }
    @Override
    String getType() {
        return NamingScheme.FOLDERS;
    }
    public abstract List<FileSystemObject> getList();
    
    
    
     public Folder getFolder(String pwd) {
        for (FileSystemObject fs:this.getList()){
            if (fs instanceof Folder){
                if(fs.getPwd().equals(pwd)){
                    return (Folder) fs;
                }else if (pwd.startsWith(fs.getPwd())){
                    return getFolder(pwd);
                } 
                    
            }
        }
        return null;
    }
     
    public abstract File getFile(String pwd,boolean create);
    public abstract File createNewFile(String relPath);
    public abstract void deleteFile(String relPath);

    public abstract boolean isExisting();
}
