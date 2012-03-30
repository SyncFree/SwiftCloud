package swift.dc;

import static sys.net.api.Networking.Networking;
import sys.Sys;

/**
 * Single server replying to client request. This class is used only to allow
 * client testing.
 * 
 * @author nmp
 * 
 */
public class DCServer {
    DCSurrogate server;
    String sequencerHost;

    public DCServer(String sequencerHost) {
        this.sequencerHost = sequencerHost;
        init();
    }

    protected void init() {

    }

    public void startSurrogServer() {
        Sys.init();

        server = new DCSurrogate(Networking.Networking.rpcBind(DCConstants.SURROGATE_PORT, null), Networking.rpcBind(0,
                null), Networking.resolve(sequencerHost, DCConstants.SEQUENCER_PORT));
    }

    public static void main(String[] args) {
        new DCServer(args.length == 0 ? "localhost" : args[0]).startSurrogServer();
    }
}
