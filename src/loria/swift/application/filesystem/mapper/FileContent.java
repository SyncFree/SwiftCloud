/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package loria.swift.application.filesystem.mapper;

/**
 * Base interface for content of a text file.
 * @author urso
 */
public interface FileContent {

    public void set(String newValue);
    
    public String getText();
}
