package swift.application;

import swift.dc.DCSequencerServer;
import swift.dc.DCServer;

/**
 * Local setup/test with one server and two clients.
 * 
 * @author annettebieniusa
 * 
 */
public class LocalSetUpTest {
    static String sequencerName = "localhost";

    static String client1Name = "c1";
    static String client2Name = "c2";

    public static void main(String[] args) {
        Thread sequencer = new Thread() {
            public void run() {
                DCSequencerServer sequencer = new DCSequencerServer(sequencerName);
                sequencer.start();
            }
        };
        sequencer.start();

        Thread server = new Thread() {
            public void run() {
                DCServer server = new DCServer(sequencerName);
                server.startSurrogServer();
            }
        };
        server.start();
    }
}
