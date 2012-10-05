/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package loria.swift.application.filesystem.mapper;

import swift.crdt.interfaces.Copyable;

/**
 * Encapsulates a String (since String are not Copyable) in order to be put into a register. 
 * @author urso
 */
public class Content implements Copyable {

    String content;

    public Content(String content) {
        this.content = content;
    }

    public String getContent() {
        return content;
    }

    @Override
    public Object copy() {
        return new Content(content);
    }
}