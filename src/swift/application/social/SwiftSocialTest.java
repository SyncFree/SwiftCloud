package swift.application.social;

import java.util.Set;

import swift.client.SwiftImpl;
import swift.crdt.interfaces.Swift;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import sys.Sys;

public class SwiftSocialTest {
    static String sequencerName = "localhost";

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

        Sys.init();
        int portId = 2001;
        Swift clientServer = SwiftImpl.newInstance(portId, "localhost", DCConstants.SURROGATE_PORT);
        SwiftSocial client = new SwiftSocial(clientServer);

        client.addUser("Biene", "Honig");
        boolean successfulLogin = client.login("Biene", "Honig");
        System.out.println("Login successful:" + successfulLogin);

        client.postMessage("Biene", "What a wonderful day!", System.currentTimeMillis());
        Set<Message> report = client.getSiteReport();
        for (Message m : report) {
            System.out.println(m);
        }

        client.sendFriendRequest("Butterfly");
        Set<String> friends = client.readUserFriends();
        for (String friend : friends) {
            System.out.println(friend);
        }

        // client.answerFriendRequest("Biene", "Butterfly", true);

    }
}
