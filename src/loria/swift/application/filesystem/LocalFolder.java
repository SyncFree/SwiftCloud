/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package loria.swift.application.filesystem;

import java.util.HashMap;
import java.util.Map;

/**
 * Local view of a folder. Contains a map of file system objects.  
 * @author urso
 */
public class LocalFolder implements FileSystemObject {
    final Map<String, FileSystemObject> content;
    final String name;
    
    public LocalFolder(String name) {
        this.name = name;
        content = new  HashMap<String, FileSystemObject>();
    }
        
    public Map<String, FileSystemObject> getContent() {
        return content;
    } 

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getType() {
        return "local_folder";
    }
}
