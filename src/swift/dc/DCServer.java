package swift.dc;

import static sys.net.api.Networking.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import sys.Sys;
import sys.net.api.rpc.RpcFactory;

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

    RpcFactory rpcFactory;
    
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

//        server = new DCSurrogate(Networking.rpcBind(portSurrogate).toDefaultService(), 
//                                Networking.rpcConnect().toDefaultService(), 
//                                Networking.resolve(sequencerHost, sequencerPort), 
//                                props);
        
        rpcFactory = Networking.rpcBind( portSurrogate);
        
        server = new DCSurrogate( rpcFactory.toDefaultService(), 
        		rpcFactory.toDefaultService(), 
                Networking.resolve(sequencerHost, sequencerPort), 
                props);
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.putAll(System.getProperties());
        if( ! props.containsKey(DCConstants.DATABASE_CLASS)) {
            if( DCConstants.DEFAULT_DB_NULL) {
                props.setProperty( DCConstants.DATABASE_CLASS, "swift.dc.db.DevNullNodeDatabase");
            } else {
                props.setProperty( DCConstants.DATABASE_CLASS, "swift.dc.db.DCBerkeleyDBDatabase");
                props.setProperty( DCConstants.BERKELEYDB_DIR, "db/default");
            }
        }
        if( ! props.containsKey(DCConstants.DATABASE_CLASS)) {
            props.setProperty(DCConstants.PRUNE_POLICY, "false");
        }
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
