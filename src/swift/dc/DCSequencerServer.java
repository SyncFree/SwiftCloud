package swift.dc;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import swift.client.proto.GenerateTimestampReply;
import swift.client.proto.GenerateTimestampRequest;
import swift.client.proto.KeepaliveReply;
import swift.client.proto.KeepaliveRequest;
import swift.client.proto.LatestKnownClockReply;
import swift.client.proto.LatestKnownClockReplyHandler;
import swift.client.proto.LatestKnownClockRequest;
import swift.clocks.CausalityClock;
import swift.clocks.ClockFactory;
import swift.clocks.IncrementalTimestampGenerator;
import swift.clocks.Timestamp;
import swift.crdt.operations.CRDTObjectOperationsGroup;
import swift.dc.proto.CommitTSReply;
import swift.dc.proto.CommitTSReplyHandler;
import swift.dc.proto.CommitTSRequest;
import swift.dc.proto.SequencerServer;
import sys.Sys;
import sys.net.api.Networking;
import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcEndpoint;

/**
 * Single server replying to client request. This class is used only to allow
 * client testing.
 * 
 * @author nmp
 * 
 */
public class DCSequencerServer extends Handler implements SequencerServer {
    RpcEndpoint endpoint;
    IncrementalTimestampGenerator clockGen;
    CausalityClock currentState;
    Map<Timestamp, Date> pendingTS;
    String siteId;
    
    Map<String,Set<CommitRecord>> ops;

    public DCSequencerServer(String siteId) {
        this.siteId = siteId;
        init();
    }

    protected void init() {
        // TODO: reinitiate clock to a correct value
        currentState = ClockFactory.newClock();
        clockGen = new IncrementalTimestampGenerator(siteId);
        pendingTS = new HashMap<Timestamp, Date>();
        ops = new HashMap<String,Set<CommitRecord>>();
    }

    public void start() {
        Sys.init();

        this.endpoint = Networking.Networking.rpcBind(DCConstants.SEQUENCER_PORT, null);
        this.endpoint.setHandler(this);
        DCConstants.DCLogger.info("Sequencer ready...");
    }
    
    private void addToOps( CommitRecord record) {
        synchronized( ops) {
            Set<CommitRecord> s = ops.get(record.baseTimestamp.getIdentifier());
            if( s == null) {
                s = new TreeSet<CommitRecord>();
                ops.put(record.baseTimestamp.getIdentifier(), s);
            }
            s.add(record);
            
        }
    }

    public static void main(String[] args) {
        new DCSequencerServer(args.length == 0 ? "X" : args[0]).start();
    }

    private synchronized Timestamp generateNewId() {
        Timestamp t = clockGen.generateNew();
        pendingTS.put(t, new Date());
        return t;
    }

    private synchronized boolean refreshId(Timestamp t) {
        boolean hasTS = pendingTS.containsKey(t);
        if (hasTS)
            pendingTS.put(t, new Date());
        return hasTS;
    }

    private synchronized boolean commitTS(CausalityClock clk, Timestamp t, boolean commit) {
        boolean hasTS = pendingTS.remove(t) != null;
        currentState.merge(clk);
        currentState.record(t);
        return hasTS;
    }

    private synchronized CausalityClock currentClockCopy() {
        return currentState.clone();
    }

    @Override
    public void onReceive(RpcConnection conn, GenerateTimestampRequest request) {
        DCConstants.DCLogger.info( "sequencer: generatetimestamprequest");
        conn.reply(new GenerateTimestampReply(generateNewId(), DCConstants.DEFAULT_TRXIDTIME));
    }

    @Override
    public void onReceive(RpcConnection conn, KeepaliveRequest request) {
        DCConstants.DCLogger.info("sequencer: keepaliverequest");
        boolean success = refreshId(request.getTimestamp());
        conn.reply(new KeepaliveReply(success, success, DCConstants.DEFAULT_TRXIDTIME));

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
        DCConstants.DCLogger.info("sequencer: latestknownclockrequest:" + currentClockCopy());
        conn.reply(new LatestKnownClockReply(currentClockCopy()));
    }

    /**
     * @param conn
     *            connection such that the remote end implements
     *            {@link CommitTSReplyHandler} and expects {@link CommitTSReply}
     * @param request
     *            request to serve
     */
    @Override
    public void onReceive(RpcConnection conn, CommitTSRequest request) {
        DCConstants.DCLogger.info("sequencer: commitTSRequest:" + request.getTimestamp());
        boolean ok = this.commitTS(request.getVersion(), request.getTimestamp(), request.getCommit());
        CausalityClock clk = currentClockCopy();
        if (ok) {
            conn.reply(new CommitTSReply(CommitTSReply.CommitTSStatus.OK, clk));
        } else {
            conn.reply(new CommitTSReply(CommitTSReply.CommitTSStatus.FAILED, clk));
        }
        addToOps( new CommitRecord( request.getObjectUpdateGroups(), request.getBaseTimestamp()));
    }
}

class CommitRecord implements Comparable<CommitRecord>
{
    List<CRDTObjectOperationsGroup<?>> objectUpdateGroups;
    Timestamp baseTimestamp;
    public CommitRecord(List<CRDTObjectOperationsGroup<?>> objectUpdateGroups, Timestamp baseTimestamp) {
        this.objectUpdateGroups = objectUpdateGroups;
        this.baseTimestamp = baseTimestamp;
    }
    @Override
    public int compareTo(CommitRecord o) {
        return (int)(baseTimestamp.getCounter() - o.baseTimestamp.getCounter());
    }
}
