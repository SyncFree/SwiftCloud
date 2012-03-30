package swift.dc;

import static sys.net.api.Networking.Networking;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import swift.client.proto.CommitUpdatesReply;
import swift.client.proto.CommitUpdatesRequest;
import swift.client.proto.FetchObjectDeltaRequest;
import swift.client.proto.FetchObjectVersionReply;
import swift.client.proto.FetchObjectVersionRequest;
import swift.client.proto.GenerateTimestampReply;
import swift.client.proto.GenerateTimestampReplyHandler;
import swift.client.proto.GenerateTimestampRequest;
import swift.client.proto.KeepaliveReply;
import swift.client.proto.KeepaliveReplyHandler;
import swift.client.proto.KeepaliveRequest;
import swift.client.proto.LatestKnownClockReply;
import swift.client.proto.LatestKnownClockReplyHandler;
import swift.client.proto.LatestKnownClockRequest;
import swift.client.proto.RecentUpdatesRequest;
import swift.client.proto.UnsubscribeUpdatesRequest;
import swift.clocks.CausalityClock;
import swift.clocks.ClockFactory;
import swift.clocks.Timestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.IntegerVersioned;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTOperation;
import swift.crdt.operations.CRDTObjectOperationsGroup;
import swift.crdt.operations.CreateObjectOperation;
import sys.Sys;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

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

    protected DCServer(String sequencerHost) {
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

