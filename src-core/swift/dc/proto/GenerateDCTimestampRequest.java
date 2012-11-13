package swift.dc.proto;

import swift.client.proto.ClientRequest;
import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

/**
 * Server request to generate a timestamp for a transaction.
 * 
 * @author nmp
 */
public class GenerateDCTimestampRequest extends ClientRequest {
    Timestamp cltTimestamp;
    CausalityClock dependencyClk;
    long cltDependency;

    // Fake constructor for Kryo serialization. Do NOT use.
    GenerateDCTimestampRequest() {
    }

    public GenerateDCTimestampRequest(String clientId, Timestamp cltTimestamp, CausalityClock dependencyClk) {
        super(clientId);
        this.cltTimestamp = cltTimestamp;
        this.dependencyClk = dependencyClk;
        cltDependency = dependencyClk.getLatestCounter(clientId);
        dependencyClk.drop(clientId);
    }


    /**
     * @return client timestamp    */
    public Timestamp getCltTimestamp() {
        return cltTimestamp;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((SequencerServer) handler).onReceive(conn, this);
    }

    public CausalityClock getDependencyClk() {
        return dependencyClk;
    }
}
