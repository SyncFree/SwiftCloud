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
package loria.RemoteControl;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;
import loria.RemoteControl.jobs.Jobs;

/**
 * lauched by amazon image.
 *
 * @author Stephane Martin <stephane.martin@loria.fr>
 */
public class RemoteControl implements Runnable {

    public static final int port = 15500;
    PrintStream out;
    LinkedList<Jobs> todo = new LinkedList();

    /*
     * Main setUp the node
     */
    public static void main(String... arg) {
        try {
            ServerSocket server = new ServerSocket(port);
            while (true) {

                Socket connect = server.accept();
                Thread th = new Thread(new RemoteControl(connect));
                th.start();
            }

        } catch (Exception ex) {
            Logger.getLogger(RemoteControl.class.getName()).log(Level.SEVERE, null, ex);
        }
        //run = false;
    }

    public RemoteControl(Socket connect) {
        this.connect = connect;
    }

    /*@Override
    public void run() {
        while (true) {
            if (todo.size() > 0) {
                //    todo.pollFirst().doOperation(connect);
            }
        }
    }*/

    //static class PCConnect implements Runnable {
        /*
         * this thread is lauched when a computer is connected
         */

        Socket connect;

       /* public PCConnect(Socket connect) {
            this.connect = connect;
        }*/

        @Override
        public void run() {
            ObjectInputStream input=null;
            ObjectOutputStream output=null;
            
            try {
                 input = new ObjectInputStream(connect.getInputStream());
                 output = new ObjectOutputStream(connect.getOutputStream());
                do {

                    Object obj = input.readObject();
                    Logger.getLogger(getClass().getName()).info("recieve: " + obj.toString());
                    if (obj instanceof Jobs) {
                        //todo.add((Jobs)obj); Why not make list of jobs ?
                        //RemoteControl.this.notify();
                        ((Jobs) obj).doOperation(output);
                    }
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
            }

        }
    //}
}
