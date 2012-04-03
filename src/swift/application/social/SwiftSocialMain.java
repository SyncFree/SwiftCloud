package swift.application.social;

import static sys.net.api.Networking.Networking;
import swift.client.SwiftImpl;
import swift.crdt.interfaces.Swift;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import sys.Sys;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcEndpoint;

public class SwiftSocialMain {
    static String sequencerName = "localhost";

    public static void main(String[] args) {
        Sys.init();

        Thread sequencer = new Thread() {
            public void run() {
                DCSequencerServer sequencer = new DCSequencerServer(sequencerName);
                sequencer.start();
            }
        };
        sequencer.start();
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // do nothing
        }

        Thread server = new Thread() {
            public void run() {
                DCServer server = new DCServer(sequencerName);
                server.startSurrogServer();
            }
        };
        server.start();
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // do nothing
        }

        int portId = 2001;
        RpcEndpoint localEP = Networking.rpcBind(portId, null);
        final Endpoint serverEP = Networking.resolve("localhost", DCConstants.SURROGATE_PORT);
        Swift clientServer = new SwiftImpl(localEP, serverEP);
        SwiftSocial client = new SwiftSocial(clientServer);
        client.addUser("Biene", "Honig");
        try {
            Thread.sleep(0);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        client.login("Biene", "Honig");
    }
}
