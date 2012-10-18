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
package loria.swift.application.filesynchroniser;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Stephane Martin <stephane.martin@loria.fr>
 */
public class SwiftSynchronizerClient implements SwiftSynchronizer {

    InetAddress serverAddress;
    Socket sock;
    ObjectInputStream input;
    ObjectOutputStream output;

    public SwiftSynchronizerClient(InetAddress serverAddress, int port) throws IOException {
        this.serverAddress = serverAddress;
        sock = new Socket(serverAddress, port);
        output = new ObjectOutputStream(sock.getOutputStream());
        input = new ObjectInputStream(sock.getInputStream());
    }
    public SwiftSynchronizerClient(String serverAddress, int port) throws IOException {
        this(InetAddress.getByName(serverAddress),port);
    }

    @Override
    public void commit(String textName, String newValue) {
        try {
            Logger.getLogger(SwiftSynchronizerClient.class.getName()).log(Level.INFO,"Send Commit "+textName+":"+newValue);
            output.writeObject(new SwiftSynchronizerServer.Commit(textName, newValue));
        } catch (IOException ex) {
            Logger.getLogger(SwiftSynchronizerClient.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public String update(String textName) {
        try {
            Logger.getLogger(SwiftSynchronizerClient.class.getName()).log(Level.INFO,"Send AskUpdate "+textName);
            output.writeObject(new SwiftSynchronizerServer.AskUpdate(textName));
            SwiftSynchronizerServer.Update up=(SwiftSynchronizerServer.Update)input.readObject();
            Logger.getLogger(SwiftSynchronizerClient.class.getName()).log(Level.INFO,"recieve update "+textName+" : "+up.getContent());
            return up.getContent();
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(SwiftSynchronizerClient.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(SwiftSynchronizerClient.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }
}
