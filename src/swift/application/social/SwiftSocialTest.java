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
        DCSequencerServer sequencer = new DCSequencerServer(sequencerName);
        sequencer.start();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // do nothing
        }

        DCServer.main(new String[] { sequencerName });
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // do nothing
        }

        Sys.init();
        int portId = 2001;
        Swift clientServer = SwiftImpl.newInstance(portId, "localhost", DCConstants.SURROGATE_PORT);
        SwiftSocial client = new SwiftSocial(clientServer);

        // Register users
        client.registerUser("Biene", "Honig", "Anne Biene", 0);
        client.registerUser("Butterfly", "Flower", "Hugo Butterfly", 0);

        // Start a session
        boolean successfulLogin = client.login("Biene", "Honig");
        System.out.println("Login successful:" + successfulLogin);

        client.postMessage("Biene", "What a wonderful day!", System.currentTimeMillis());
        client.postMessage("Biene", "Need more chocolate!", System.currentTimeMillis());
        client.postMessage("Butterfly", "Ready for springtime?", System.currentTimeMillis());

        Set<Message> report = client.getMessagesFor("Biene");
        System.out.println("Messages:");
        for (Message m : report) {
            System.out.println(m);
        }

        client.sendFriendRequest("Butterfly");
        Set<String> friends = client.readUserFriends("Biene");
        System.out.println("Friends:");
        for (String friend : friends) {
            System.out.println(friend);
        }

        client.logout("Biene");

        // Start another session with different user
        successfulLogin = client.login("Butterfly", "Flower");
        System.out.println("Login successful:" + successfulLogin);
        client.answerFriendRequest("Biene", true);

        report = client.getMessagesFor("Biene");
        for (Message m : report) {
            System.out.println("Messages:");
            System.out.println(m);
        }

        friends = client.readUserFriends("Butterfly");
        System.out.println("Friends of Butterfly:");
        for (String friend : friends) {
            System.out.println(friend);
        }
        friends = client.readUserFriends("Biene");
        System.out.println("Friends of Biene:");
        for (String friend : friends) {
            System.out.println(friend);
        }
        client.logout("Butterfly");

        clientServer.stop(true);
    }
}
