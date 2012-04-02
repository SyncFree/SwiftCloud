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
    public static void main(String[] args) {
        Thread sequencer = new Thread() {
            public void run() {
                DCSequencerServer sequencer = new DCSequencerServer("seq");
                sequencer.start();
            }
        };
        sequencer.start();

        Thread server = new Thread() {
            public void run() {
                DCServer server = new DCServer("seq");
                server.startSurrogServer();
            }
        };
        server.start();

        int portId = 2001;
        Sys.init();
        RpcEndpoint localEP = Networking.rpcBind(portId, null);
        final Endpoint serverEP = Networking.resolve("localhost", DCConstants.SURROGATE_PORT);
        Swift clientServer = new SwiftImpl(localEP, serverEP);
        SwiftSocial client = new SwiftSocial(clientServer);
        client.addUser("Biene", "Honig");
        client.login("Biene", "Honig");
    }
}
