package swift.application.social;

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

import swift.client.SwiftImpl;
import swift.crdt.interfaces.Swift;
import swift.dc.DCConstants;
import sys.Sys;

public class SwiftSocialTest {
    static String sequencerName = "localhost";

    public static void main(String[] args) {
        // DCSequencerServer sequencer = new DCSequencerServer(sequencerName);
        // sequencer.start();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // do nothing
        }

        // DCServer.main(new String[] { sequencerName });
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
        client.registerUser("Biene", "Honig", "Anne Biene", 0, System.currentTimeMillis());
        client.registerUser("Butterfly", "Flower", "Hugo Butterfly", 0, System.currentTimeMillis());

        // Start a session
        boolean successfulLogin = client.login("Biene", "Honig");
        System.out.println("Login successful:" + successfulLogin);

        client.updateStatus("What a wonderful day!", System.currentTimeMillis());
        client.updateStatus("Need more chocolate!", System.currentTimeMillis());
        client.postMessage("Butterfly", "Ready for springtime?", System.currentTimeMillis());

        Set<Message> messages = new TreeSet<Message>();
        Set<Message> events = new TreeSet<Message>();

        client.read("Biene", messages, events);
        System.out.println("Messages:");
        for (Message m : messages) {
            System.out.println(m);
        }
        System.out.println("Events:");
        for (Message m : events) {
            System.out.println(m);
        }

        client.sendFriendRequest("Butterfly");
        Set<Friend> friends = client.readFriendList("Biene");
        System.out.println("Friends:");
        for (Friend friend : friends) {
            System.out.println(friend);
        }

        client.logout("Biene");

        // Start another session with different user
        successfulLogin = client.login("Butterfly", "Flower");
        System.out.println("Login successful:" + successfulLogin);
        client.answerFriendRequest("Biene", true);

        Collection<Message> msgs = new TreeSet<Message>();
        Collection<Message> evnts = new TreeSet<Message>();
        User user = client.read("Butterfly", msgs, evnts);
        System.out.println("User : " + user.userInfo());
        System.out.println("Messages:");
        for (Message m : msgs) {
            System.out.println(m);
        }
        System.out.println("Events:");
        for (Message m : evnts) {
            System.out.println(m);
        }

        friends = client.readFriendList("Butterfly");
        System.out.println("Friends of Butterfly:");
        for (Friend friend : friends) {
            System.out.println(friend);
        }
        friends = client.readFriendList("Biene");
        System.out.println("Friends of Biene:");
        for (Friend friend : friends) {
            System.out.println(friend);
        }
        client.logout("Butterfly");

        clientServer.stop(true);
    }
}
