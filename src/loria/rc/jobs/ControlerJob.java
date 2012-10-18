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
package loria.rc.jobs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import loria.rc.AmazonMachine;
import loria.rc.RemoteControl;
import loria.rc.info.Status;

/**
 * This job consists to mount micro instance, and lauch them
 *
 * @author Stephane Martin <stephane.martin@loria.fr>
 */
public class ControlerJob extends Jobs {

    private AtomicInteger finished;
    int numberOfClient;
    int numberOfScout;
    boolean scoutIsOnClient = false;
    int number;
    List<Machine> machines = new LinkedList();
    Machine swiftMachine;
    Jobs clientJob;
    List<AmazonMachine> groupMachine = new LinkedList();
    int increment = 10;
    int nbMaxMachine = 1000;

    public void givejobs() {
        finished.set(0);
        for (Machine ma : machines) {
            ma.sendJob(clientJob);
        }

    }

    public void fillMachines(AmazonMachine am) throws IOException {
        Logger.getLogger(ControlerJob.class.getName()).info("Waiting started");
        am.waitAllLauched();
        for (InetAddress in : am.getInetAddress()) {
            Machine ma = new Machine(in, this.finished);
            machines.add(ma);
        }
    }

    void newMachineAndnewJob(AmazonMachine am, String name) throws Exception {
        this.clientJob.setJobName(name);
        groupMachine.add(am);
        fillMachines(am);
        Logger.getLogger(ControlerJob.class.getName()).info("Give jobs");
        givejobs();
        Logger.getLogger(ControlerJob.class.getName()).info("Waiting finished...");
        while (finished.get() < machines.size()) {
            finished.wait();
        }
        Logger.getLogger(ControlerJob.class.getName()).info("finished");
    }

    @Override
    public void run() {
        try {
            Logger.getLogger(ControlerJob.class.getName()).info("Start scenario");
            AmazonMachine swiftam = new AmazonMachine();

            swiftam.checkAndCreateSecurityGroup();
            swiftam.startInstanceRequest(1);
            Logger.getLogger(ControlerJob.class.getName()).info("Start swiftMachine");
            AmazonMachine am = new AmazonMachine();


            Logger.getLogger(ControlerJob.class.getName()).info("Start First increment client");
            am.startInstanceRequest(increment);
            Logger.getLogger(ControlerJob.class.getName()).info("Waiting Swift...");
            swiftam.waitAllLauched();
            groupMachine.add(swiftam);
            clientJob.destHostName = swiftam.getInetAddress().get(0).getHostName();

            Logger.getLogger(ControlerJob.class.getName()).info("Swift is started, send a job");

            SwiftCloudServerJob job1 = new SwiftCloudServerJob();
            swiftMachine = new Machine(swiftam.getInetAddress().get(0), this.finished);
            swiftMachine.sendJob(job1);
            Logger.getLogger(ControlerJob.class.getName()).info("Waiting Swift jobs is ready...");
            swiftMachine.waitReady();

            /* ---------------------------- */

            Logger.getLogger(ControlerJob.class.getName()).info("Start client jobs essais 1");
            newMachineAndnewJob(am, "essais 1");

            for (int i = 2; i * increment < number; i++) {
                Logger.getLogger(ControlerJob.class.getName()).info("Start client jobs essais " + i);
                am = new AmazonMachine();
                am.startInstanceRequest(increment);
                newMachineAndnewJob(am, "essais " + i);
            }

            /* ---- End of experiment finish all machine --- */


        } catch (Exception ex) {
            Logger.getLogger(ControlerJob.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            Logger.getLogger(ControlerJob.class.getName()).info("Clean Up");
            for (AmazonMachine ams : this.groupMachine) {
                ams.cleanupInstances();
            }
            this.sendObejct(Status.FINISHED);
        }
    }

    public static class Machine implements Runnable {

        private Thread th;
        private Socket sock;
        private ObjectInputStream input;
        private ObjectOutputStream output;
        private boolean run = true;
        private boolean ready = false;
        private final int maxTry = 5;
        private AtomicInteger finished;

        void init() throws IOException {
            output = new ObjectOutputStream(sock.getOutputStream());
            input = new ObjectInputStream(sock.getInputStream());
            th = new Thread(this);
            th.start();

        }

        public Machine(Socket sock, AtomicInteger finished) throws IOException {
            this.sock = sock;
            init();

        }

        public Machine(InetAddress addr, AtomicInteger finished) throws IOException {
            for (int i = 0; i < maxTry; i++) {
                try {
                    sock = new Socket(addr, RemoteControl.PORT);
                    init();
                    break;
                } catch (Exception ex) {
                    Logger.getLogger(ControlerJob.class.getName()).log(Level.SEVERE, null, ex);
                }
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException ex) {
                    Logger.getLogger(ControlerJob.class.getName()).log(Level.SEVERE, null, ex);

                }
            }
            if (sock == null) {
                throw new IOException("Could not connect !");
            }

        }

        public Machine(String name, AtomicInteger finished) throws IOException {
            this(InetAddress.getByName(name), finished);
        }

        public void sendJob(Jobs job) {
            try {
                output.writeObject(job);
            } catch (IOException ex) {
                Logger.getLogger(ControlerJob.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        @Override
        public void run() {
            run = true;
            while (run) {
                try {

                    Object obj = input.readObject();
                    if (obj instanceof Status) {
                        switch (((Status) obj).getState()) {
                            case Finished:
                                if (finished != null) {
                                    finished.incrementAndGet();
                                    finished.notifyAll();
                                }
                                break;
                            case Ready:
                                synchronized (this) {
                                    ready = true;
                                    notifyAll();
                                }
                        }

                    }
                } catch (ClassNotFoundException ex) {
                    Logger.getLogger(ControlerJob.class.getName()).log(Level.SEVERE, null, ex);

                } catch (IOException ex) {
                }
            }
        }

        synchronized public void waitReady() {
            while (!this.ready) {
                try {
                    this.wait();
                } catch (InterruptedException ex) {
                    Logger.getLogger(ControlerJob.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }
}
