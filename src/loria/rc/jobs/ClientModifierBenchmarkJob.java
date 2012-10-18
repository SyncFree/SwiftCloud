package loria.rc.jobs;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import loria.rc.AmazonS3Upload;
import loria.rc.info.Status;
import loria.swift.application.filesynchroniser.StandardDiffProfile;
import loria.swift.application.filesynchroniser.SwiftSynchronizer;
import loria.swift.application.filesynchroniser.SwiftSynchronizerClient;
import loria.swift.application.filesynchroniser.SwiftSynchronizerDirect;
import loria.swift.application.filesystem.mapper.RegisterFileContent;
import loria.swift.crdt.logoot.LogootVersioned;
import swift.client.SwiftImpl;
import swift.client.SwiftOptions;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.SwiftSession;
import swift.dc.DCConstants;

/**
 * This jobs simulate a client
 *
 * @author Stephane Martin <stephane.martin@loria.fr>
 */
public class ClientModifierBenchmarkJob extends Jobs implements Runnable {

    Class classes[] = {LogootVersioned.class, RegisterFileContent.class};
    // private String scoutName = "localhost";

    public static enum Type {

        Logout, LastWriterWin, Remote
    }
    private SwiftSynchronizer sync;
    private StandardDiffProfile profile;
    List<String> filesList;
    int sleep = 0;
    int IndirectPort = 5658;
    Type type;
    SwiftSession server;
    int maxFileNumber = 5;
    int numberOfCycle = 0;
    double probAddFile = 0.6;
    LinkedList<Task> todoList = new LinkedList();
    /* LinkedList<Long> updateTimes = new LinkedList();
     LinkedList<Long> commitTimes = new LinkedList();*/
    boolean aSynch = false;
    PrintStream out;
    File outputFile;
    /*
     * 
     * Jobs execution
     * init file 
     * Connect to scout 
     * lauch the LOOP !
     * at end send all stat to S3
     * and terminate the machine
     */

    @Override
    public void run() {
        if (type == Type.Remote) {
            try {
                sync = new SwiftSynchronizerClient(this.destHostName, IndirectPort);
            } catch (IOException ex) {
                Logger.getLogger(ClientModifierBenchmarkJob.class.getName()).log(Level.SEVERE, null, ex);
                return;
            }
        } else {
            server = SwiftImpl.newSingleSessionInstance(new SwiftOptions(this.destHostName, DCConstants.SURROGATE_PORT));
            sync = new SwiftSynchronizerDirect(server, IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, true, aSynch, classes[type.ordinal()]);
        }
        profile = StandardDiffProfile.GIT;
        try {
            initFile();

            run = true;
            while (isRunning() && numberOfCycle != 0) {
                Task f = todoList.pollFirst();
                f.updateOrModifyAndCommit();
                todoList.addLast(f);
                addFile();
                sleep();
                numberOfCycle--;
            }
            send2S3();
            sendObejct(Status.FINISHED);
        } catch (Exception ex) {
            Logger.getLogger(ClientModifierBenchmarkJob.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    /*
     * Make operation with type : logoot or register and uses the scout
     */
    public ClientModifierBenchmarkJob(Type type,int numberOfCycle) {
        this.type = type;
        this.numberOfCycle=numberOfCycle;

    }

    public ClientModifierBenchmarkJob(Type type,int numberOfCycle, boolean aSynch, int sleep, int docMax) {
        this(type,numberOfCycle);
        this.sleep = sleep;
        this.maxFileNumber = docMax;
        this.aSynch = aSynch;
    }

    public void setNumberOfCycle(int numberOfCycle) {
        this.numberOfCycle = numberOfCycle;
    }

    void initFile() throws Exception {
        outputFile = new File("" + System.getProperty("user.home")
                + "/" + this.type + "." + this.getJobName() + "."
                + InetAddress.getLocalHost().getHostName() + ".csv");
        out = new PrintStream(outputFile);
    }

    void send2S3() {
        out.flush();
        out.close();
        AmazonS3Upload s3 = new AmazonS3Upload();
        s3.uploadFile(outputFile, outputFile.getName());
    }
    /*
     * Just sleep if it is setted
     */

    private void sleep() {
        if (sleep > 0) {
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException ex) {
                Logger.getLogger(ClientModifierBenchmarkJob.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    /*
     * Add file on todoList
     */
    private void addFile() {
        if (maxFileNumber < todoList.size() && Math.random() > probAddFile) {
            Task f = new Task(new Integer(todoList.size() - 1).toString());
            todoList.addLast(f);
        }
    }

    /*
     * task represent a file which edited.
     */
    class Task {

        String name;
        String content;
        boolean flip = false;

        public Task(String name) {
            this.name = name;
            this.content = "";
        }

        void updateOrModifyAndCommit() {
            if (flip) {
                update();
                flip = false;
            } else {
                modifAndCommit();
                flip = true;
            }
        }

        void update() {
            long begin = System.currentTimeMillis();
            content = sync.update(name);
            long end = System.currentTimeMillis();
            addUpdateTime(end - begin, content.length());

        }

        void modifAndCommit() {
            content = profile.change(content);
            long begin = System.currentTimeMillis();
            sync.commit(name, content);
            long end = System.currentTimeMillis();
            addCommitTime(end - begin, content.length());
        }

        void addUpdateTime(long l, int size) {
            //ClientModifierBenchmarkJob.this.updateTimes.add(new Long(l));
            out.println("update;" + name + ";" + l + ";" + size);
        }

        void addCommitTime(long l, int size) {
            //ClientModifierBenchmarkJob.this.commitTimes.add(new Long(l));
            out.println("commit;" + name + ";" + l + ";" + size);
        }
    }
}
