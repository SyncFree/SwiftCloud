package swift.dc;

import swift.client.proto.CommitUpdatesRequest;
import swift.client.proto.FetchObjectDeltaRequest;
import swift.client.proto.FetchObjectVersionRequest;
import swift.client.proto.GenerateTimestampReply;
import swift.client.proto.GenerateTimestampRequest;
import swift.client.proto.SequencerServer;
import swift.client.proto.KeepaliveReply;
import swift.client.proto.KeepaliveRequest;
import swift.client.proto.LatestKnownClockReply;
import swift.client.proto.LatestKnownClockReplyHandler;
import swift.client.proto.LatestKnownClockRequest;
import swift.clocks.CausalityClock;
import swift.clocks.ClockFactory;
import swift.clocks.IncrementalTimestampGenerator;
import swift.clocks.Timestamp;
import swift.clocks.VersionVectorWithExceptions;
import sys.Sys;
import sys.net.api.Networking;
import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;
import java.util.*;

/**
 * Single server replying to client request. This class is used only to allow
 * client testing.
 * 
 * @author nmp
 *
 */
public class DCSequencerServer extends Handler  implements SequencerServer {
    RpcEndpoint endpoint;
    IncrementalTimestampGenerator clockGen;
    VersionVectorWithExceptions currentState;
    Map<Timestamp,Date> pendingTS;
    String siteId;

    protected DCSequencerServer( String siteId) {
        this.siteId = siteId;
        init();
    }
    
    protected void init() {
        //TODO: reinitiate clock to a correct value
        currentState = (VersionVectorWithExceptions) ClockFactory.newClock();
        clockGen = new IncrementalTimestampGenerator( siteId);
        pendingTS = new HashMap<Timestamp,Date>();
    }
    
    public void start() {
        Sys.init();
        
        this.endpoint = Networking.Networking.rpcBind(DCConstants.SEQUENCER_PORT, null);
        this.endpoint.setHandler(this);
        System.out.println("Sequencer ready...");
    }
    
    public static void main( String []args) {
        new DCSequencerServer( args.length == 0 ? "X" : args[0]).start();
    }
    
    private synchronized Timestamp generateNewId() {
        Timestamp t = clockGen.generateNew();
        pendingTS.put(t, new Date());
        return t;
    }

    private synchronized boolean refreshId( Timestamp t) {
        boolean hasTS = pendingTS.containsKey( t);
        if( hasTS)
            pendingTS.put(t, new Date());
        return hasTS;
    }

    private synchronized CausalityClock currentClock() {
        return currentState.clone();
    }

    @Override
    public void onReceive(RpcConnection conn, GenerateTimestampRequest request) {
        System.out.println( "sequencer: generatetimestamprequest");
        conn.reply( new GenerateTimestampReply( generateNewId(), DCConstants.DEFAULT_TRXIDTIME));
    }

    @Override
    public void onReceive(RpcConnection conn, KeepaliveRequest request) {
        // TODO Auto-generated method stub
        System.out.println( "sequencer: keepaliverequest");
        boolean success = refreshId( request.getTimestamp());
        conn.reply( new KeepaliveReply( success, success, DCConstants.DEFAULT_TRXIDTIME));
        
    }
    /**
     * @param conn
     *            connection such that the remote end implements
     *            {@link LatestKnownClockReplyHandler} and expects
     *            {@link LatestKnownClockReply}
     * @param request
     *            request to serve
     */
    public void onReceive(RpcConnection conn, LatestKnownClockRequest request) {
        System.out.println( "sequencer: latestknownclockrequest");
        conn.reply( new LatestKnownClockReply( currentClock()));
    }
}
