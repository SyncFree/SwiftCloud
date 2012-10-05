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

    // Fake constructor for Kryo serialization. Do NOT use.
    GenerateDCTimestampRequest() {
    }

    public GenerateDCTimestampRequest(String clientId, Timestamp cltTimestamp) {
        super(clientId);
        this.cltTimestamp = cltTimestamp;
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
}
