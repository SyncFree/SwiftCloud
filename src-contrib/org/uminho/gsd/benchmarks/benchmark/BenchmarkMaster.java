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
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class BenchmarkMaster {

    private int PersonalClientID;
    Map<String, Object> slaves;
    BenchmarkExecutor executor;
    ArrayList<SlaveHandler> slaveHandlers;
    CountDownLatch countBarrier;

    public BenchmarkMaster(BenchmarkExecutor executor, Map<String, Object> slaves) {
        this.executor = executor;
        this.slaves = slaves;
    }

    public void run() throws Exception {

        slaveHandlers = new ArrayList<SlaveHandler>();
        PersonalClientID = 1;
        countBarrier = new CountDownLatch(slaves.size());

        final Exception[] error = new Exception[1];
        error[0] = null;

        Runnable run = new Runnable() {
            public void run() {
                try {
                    executor.prepare();
                } catch (Exception e) {
                    error[0] = e;
                }
            }
        };

        Thread prepare_thread = new Thread(run);
        prepare_thread.start();

        //Time to the clients to be ready
        Thread.sleep(500);


        int clientId = 2;
        for (String host : slaves.keySet()) {
            SlaveHandler sh = new SlaveHandler(clientId, host, (String) slaves.get(host));
            slaveHandlers.add(sh);
            Thread t = new Thread(sh);
            t.start();
            clientId++;
        }
        try {
            System.out.println("[INFO:] Waiting for slaves");
            countBarrier.await();
            prepare_thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        if(error[0]!=null){
            throw error[0];
        }



        System.out.println("[INFO:] STARTING SLAVES");
        for (SlaveHandler sh : slaveHandlers) {
            sh.sendMessage("START\n");
        }


        countBarrier = new CountDownLatch(slaves.size());

        ensureEnd end = new ensureEnd();
        Thread ensureEndThread = new Thread(end);
        ensureEndThread.start();

        executor.run(new BenchmarkNodeID(PersonalClientID));
        System.out.println("[INFO:]SLAVE ENDED");
        try {
            countBarrier.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        executor.consolidate();

        end.killServer();
    }


    class SlaveHandler implements Runnable {


        String host;
        int port;
        PrintWriter writer;

        BufferedReader in;
        String ClientID = "";

        SlaveHandler(int clientId, String host, String port) {
            this.ClientID = clientId + "";
            this.host = host;
            this.port = Integer.parseInt(port.trim());


            try {
                Socket cs = new Socket(host, this.port);
                writer = new PrintWriter(cs.getOutputStream(), true);
                in = new BufferedReader(
                        new InputStreamReader(
                                cs.getInputStream()));
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }

        public void run() {

            writer.write("PREPARE " + ClientID + "\n");
            writer.flush();
            boolean terminated = false;
            while (!terminated) {
                try {
                    String message = in.readLine();
                    if (message != null && message.equalsIgnoreCase("ACK")) {
                        terminated = true;
                    }
                } catch (IOException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }

            countBarrier.countDown();

            terminated = false;
            while (!terminated) {
                try {
                    String message = in.readLine();
                    if (message != null && message.equalsIgnoreCase("EXECUTED")) {
                        System.out.println("SLAVE +" + ClientID + "+ENDED");
                        terminated = true;
                    }
                } catch (IOException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
            countBarrier.countDown();
            try {
                countBarrier.await();
            } catch (InterruptedException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            writer.write("ACK");
            writer.flush();
        }

        public void sendMessage(String message) {
            writer.write(message + "\n");
            writer.flush();
        }

    }


    class ensureEnd implements Runnable {

        ServerSocket s;

        public void run() {
            try {
                s = new ServerSocket(64546);
                Socket clientSocket = s.accept();
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(
                                clientSocket.getInputStream()));
                boolean terminated = false;

                while (!terminated) {
                    String message = in.readLine();

                    if (message != null && message.equalsIgnoreCase("KILL")) {
                        for (SlaveHandler sh : slaveHandlers) {    //kill slaves
                            sh.sendMessage("KILL\n");
                        }
                        Socket s = new Socket("localhost", 64446); //kill himself
                        PrintWriter writer = new PrintWriter(s.getOutputStream(), true);
                        writer.write("KILL\n");
                        writer.flush();
                        terminated = true;
                    }
                }
                killServer();
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }

        public void killServer() {
            try {
                s.close();
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }

        }
    }
}



