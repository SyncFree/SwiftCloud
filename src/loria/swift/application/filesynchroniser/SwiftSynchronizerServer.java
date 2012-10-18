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
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;
import loria.rc.RemoteControl;
import loria.rc.jobs.Jobs;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.SwiftSession;

/**
 *
 * @author Stephane Martin <stephane.martin@loria.fr>
 */
public class SwiftSynchronizerServer implements Runnable {

    SwiftSynchronizerDirect ssd;
    int port = 5658;
    ServerSocket server;
    Thread th;

    public SwiftSynchronizerServer(SwiftSession clientServer, IsolationLevel isolationLevel, CachePolicy cachePolicy,
            boolean subscribeUpdates, boolean asyncCommit, Class textClass) {
        ssd = new SwiftSynchronizerDirect(clientServer, isolationLevel, cachePolicy, subscribeUpdates, asyncCommit, textClass);



    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getPort() {
        return port;
    }

    public void start() {
        th = new Thread(this);
        th.start();
    }

    public void stop() {
        try {
            server.close();
        } catch (IOException ex) {
            Logger.getLogger(SwiftSynchronizerServer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void run() {
        Logger.getLogger(getClass().getName()).info("Server is ready");

        try {
            server = new ServerSocket(port);
            while (true) {
                Socket connect = server.accept();
                Logger.getLogger(getClass().getName()).info("Connected to : " + connect.getInetAddress().getCanonicalHostName());
                Thread th = new Thread(new ConnectedServer(connect));
                th.start();
            }

        } catch (Exception ex) {
            Logger.getLogger(RemoteControl.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static interface Command extends Serializable {
        //public void apply(SwiftSynchronizerDirect ssd);
    }

    public static abstract class Connected implements Runnable {

        Socket sock;
        ObjectInputStream input;
        ObjectOutputStream output;

        public Connected(Socket sock) {
            this.sock = sock;
        }

        abstract void interpret(Object obj) throws Exception;

        @Override
        public void run() {
            try {

                input = new ObjectInputStream(sock.getInputStream());
                output = new ObjectOutputStream(sock.getOutputStream());
                do {
                    Logger.getLogger(getClass().getName()).info("reading " );
                    Object obj = input.readObject();
                    Logger.getLogger(getClass().getName()).info("recieve: " + obj.toString());
                    interpret(obj);
                } while (true);
            } catch (Exception ex) {
                Logger.getLogger(RemoteControl.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                try {
                    input.close();
                } catch (IOException ex) {
                    Logger.getLogger(RemoteControl.class.getName()).log(Level.SEVERE, null, ex);
                }
                try {
                    output.close();
                } catch (IOException ex) {
                    Logger.getLogger(RemoteControl.class.getName()).log(Level.SEVERE, null, ex);
                }
                try {
                    sock.close();
                } catch (IOException ex) {
                    Logger.getLogger(SwiftSynchronizerServer.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

        }
    }

    public class ConnectedServer extends Connected {

        public ConnectedServer(Socket sock) {
            super(sock);
        }

        @Override
        void interpret(Object obj) throws Exception {
            if (obj instanceof Commit) {
                Commit c = (Commit) obj;
                Logger.getLogger(SwiftSynchronizerClient.class.getName()).log(Level.INFO, "Commit recieved " + c.getName() + ":" + c.getContent());
                ssd.commit(c.getName(), c.getContent());
                Logger.getLogger(SwiftSynchronizerClient.class.getName()).log(Level.INFO, "Commited to swift ");
            } else if (obj instanceof AskUpdate) {
                AskUpdate up = (AskUpdate) obj;
                Logger.getLogger(SwiftSynchronizerClient.class.getName()).log(Level.INFO, "AskUpdate recieved " + up.getFileName());
                output.writeObject(new Update(up.fileName, ssd.update(up.fileName)));
                Logger.getLogger(SwiftSynchronizerClient.class.getName()).log(Level.INFO, "Send update" + up.getFileName());
            }
        }
    }

    public static class Commit implements Command {

        String name;
        String content;

        public Commit(String name, String content) {
            this.name = name;
            this.content = content;
        }

        public String getName() {
            return name;
        }

        public String getContent() {
            return content;
        }
    }

    public static class AskUpdate implements Command {

        String fileName;

        public AskUpdate(String fileName) {
            this.fileName = fileName;
        }

        public String getFileName() {
            return fileName;
        }
    }

    public static class Update implements Command {

        String name;
        String content;

        public Update(String name, String content) {
            this.name = name;
            this.content = content;
        }

        public String getName() {
            return name;
        }

        public String getContent() {
            return content;
        }
    }
}
