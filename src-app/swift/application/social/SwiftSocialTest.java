/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package swift.application.social;

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

import swift.client.SwiftImpl;
import swift.client.SwiftOptions;
import swift.crdt.core.CachePolicy;
import swift.crdt.core.IsolationLevel;
import swift.crdt.core.SwiftSession;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import sys.Sys;

public class SwiftSocialTest {
    private static String sequencerName = "localhost";

    public static void main(String[] args) {
        DCSequencerServer.main(new String[] { "-name", sequencerName });
        DCServer.main(new String[] { sequencerName });

        Sys.init();
        SwiftSession clientServer = SwiftImpl.newSingleSessionInstance(new SwiftOptions("localhost",
                DCConstants.SURROGATE_PORT));
        SwiftSocial client = new SwiftSocial(clientServer, IsolationLevel.SNAPSHOT_ISOLATION,
                CachePolicy.STRICTLY_MOST_RECENT, false, false);

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

        clientServer.stopScout(true);
        System.exit(0);
    }
}
