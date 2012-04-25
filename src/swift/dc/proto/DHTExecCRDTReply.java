package swift.dc.proto;

import swift.dc.*;
import sys.dht.api.DHT;

/**
 * 
 * @author preguica
 * 
 */
public class DHTExecCRDTReply implements DHT.Reply {

    boolean result;

    /**
     * Needed for Kryo serialization
     */
    public DHTExecCRDTReply() {
    }

    public DHTExecCRDTReply(boolean result) {
        this.result = result;
    }

    @Override
    public void deliverTo(DHT.Connection conn, DHT.ReplyHandler handler) {
        if (conn.expectingReply())
            ((DHTExecCRDTReplyHandler) handler).onReceive(conn, this);
        else
            ((DHTExecCRDTReplyHandler) handler).onReceive(this);
    }

    public boolean isResult() {
        return result;
    }

}
