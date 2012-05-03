package swift.dc;

import static sys.net.api.Networking.Networking;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

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
    int sequencerPort;
    Properties props;

    public DCServer(String sequencerHost, Properties props) {
        this.sequencerHost = sequencerHost;
        int pos = sequencerHost.indexOf(":");
        if (pos != -1) {
            this.sequencerPort = Integer.parseInt(sequencerHost.substring(pos + 1));
            this.sequencerHost = sequencerHost.substring(0, pos);
        } else
            this.sequencerPort = DCConstants.SEQUENCER_PORT;
        this.props = props;
        init();
    }

    protected void init() {

    }

    public void startSurrogServer( int portSurrogate) {
        Sys.init();

        server = new DCSurrogate(Networking.Networking.rpcBind(portSurrogate, null), 
                                Networking.rpcBind(0,null), 
                                Networking.resolve(sequencerHost, sequencerPort), 
                                props);
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty( DCConstants.DATABASE_CLASS, "swift.dc.db.DevNullNodeDatabase");
        props.setProperty(DCConstants.PRUNE_POLICY, "false");
        int portSurrogate = DCConstants.SURROGATE_PORT;

        String sequencerNode = "localhost";
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-sequencer")) {
                sequencerNode = args[++i];
            } else if (args[i].startsWith("-prop:")) {
                props.setProperty( args[i].substring(6), args[++i]);
            } else if (args[i].equals("-portSurrogate")) {
                portSurrogate = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-prune")) {
                props.setProperty(DCConstants.PRUNE_POLICY, "true");
            }
        }
        
        new DCServer(sequencerNode, props).startSurrogServer( portSurrogate);
    }
}
