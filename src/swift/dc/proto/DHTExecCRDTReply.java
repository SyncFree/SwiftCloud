package swift.dc.proto;

import swift.client.proto.FastRecentUpdatesReply.ObjectSubscriptionInfo;
import swift.dc.*;
import sys.dht.api.DHT;

/**
 * 
 * @author preguica
 * 
 */
public class DHTExecCRDTReply implements DHT.Reply {
    ExecCRDTResult result;

    public DHTExecCRDTReply(ExecCRDTResult result) {
        this.result = result;
    }

    /**
     * Needed for Kryo serialization
     */
    DHTExecCRDTReply() {
    }

    @Override
    public void deliverTo(DHT.Handle conn, DHT.ReplyHandler handler) {
        if (conn.expectingReply())
            ((DHTExecCRDTReplyHandler) handler).onReceive(conn, this);
        else
            ((DHTExecCRDTReplyHandler) handler).onReceive(this);
    }

    public ExecCRDTResult getResult() {
        return result;
    }

}
