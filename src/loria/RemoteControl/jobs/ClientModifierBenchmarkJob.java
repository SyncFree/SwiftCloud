package loria.RemoteControl.jobs;

import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import loria.swift.application.filesynchroniser.StandardDiffProfile;
import loria.swift.application.filesynchroniser.SwiftSynchronizer;
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
    private String scoutName = "localhost";

    /*
     * 
     * Jobs execution
     * Connect to scout and lauch the LOOP !
     */
    @Override
    public void run() {
        server = SwiftImpl.newSingleSessionInstance(new SwiftOptions(scoutName, DCConstants.SURROGATE_PORT));
        sync = new SwiftSynchronizer(server, IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, true, false, classes[type.ordinal()]);
        profile = StandardDiffProfile.GIT;

        run = true;
        while (isRunning()) {
            Task f = todoList.pollFirst();
            f.updateOrModifyAndCommit();
            todoList.addLast(f);
            addFile();
            sleep();
        }
    }

    public static enum Type {

        Logout, LastWriterWin
    }
    private SwiftSynchronizer sync;
    private StandardDiffProfile profile;
    List<String> filesList;
    int sleep = 0;
    Type type;
    SwiftSession server;
    int maxFileNumber = 30;
    double probAddFile = 0.6;
    LinkedList<Task> todoList = new LinkedList();
    LinkedList<Long> updateTimes = new LinkedList();
    LinkedList<Long> commitTimes = new LinkedList();

    /*
     * Make operation with type : logoot or register and uses the scout
     */
    public ClientModifierBenchmarkJob(Type type, String scoutName) {
        this.type = type;
        this.scoutName = scoutName;
    }

    public ClientModifierBenchmarkJob(Type type, String scoutName, int sleep, int docMax) {
        this(type, scoutName);
        this.sleep = sleep;
        this.maxFileNumber = docMax;
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
            ClientModifierBenchmarkJob.this.updateTimes.add(new Long(end - begin));
        }

        void modifAndCommit() {
            sync.commit(name, profile.change(content));
        }
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
}
