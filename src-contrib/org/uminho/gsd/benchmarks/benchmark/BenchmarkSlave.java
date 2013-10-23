/*
 * *********************************************************************
 * Copyright (c) 2010 Pedro Gomes and Universidade do Minho.
 * All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * ********************************************************************
 */

package org.uminho.gsd.benchmarks.benchmark;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class BenchmarkSlave {

    public static boolean terminated = false;
    private int PersonalClientID;
    private int port;
    private PrintWriter writer;
    private BufferedReader in;
    private BenchmarkExecutor executor;

    BenchmarkSlave(int port, BenchmarkExecutor executor) {
        this.port = port;
        this.executor = executor;
    }

    public void run() throws Exception {
        try {
            ServerSocket ss = new ServerSocket(port);
            System.out.println("[INFO:] Slave waiting");
            Socket clientSocket = ss.accept();
            writer = new PrintWriter(clientSocket.getOutputStream(), true);
            in = new BufferedReader(
                    new InputStreamReader(
                            clientSocket.getInputStream()));
            while (!terminated) {
                String message = in.readLine();

                if (message != null && message.toUpperCase().startsWith("PREPARE")) {
                    executor.prepare();
                    String PersonalClientID_info = message.split(" ")[1];
                    PersonalClientID = Integer.parseInt(PersonalClientID_info);
                    System.out.println("[INFO:] PREPARED");
                    writer.write("ACK\n");
                    writer.flush();
                }
                if (message != null && message.equalsIgnoreCase("START")) {


                    Runnable run = new Runnable() {
                        public void run() {
                            executor.run(new BenchmarkNodeID(PersonalClientID));
                            System.out.println("[INFO:]EXECUTION ENDED ON SLAVE");
                            writer.write("EXECUTED\n");
                            writer.flush();
                        }
                    };
                    Thread t = new Thread(run);
                    t.start();


                }
                if (message != null && message.equalsIgnoreCase("ACK")) {
                    executor.consolidate();
                }
                if (message != null && message.equalsIgnoreCase("KILL")) {
                    Socket s = new Socket("localhost", 64446);
                    PrintWriter writer = new PrintWriter(s.getOutputStream(), true);
                    writer.write("KILL\n");
                    writer.flush();
                }
            }

        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }


}
