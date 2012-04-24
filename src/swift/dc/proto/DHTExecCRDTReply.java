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
            ((DHTDataNode.ReplyHandler) handler).onReceive(conn, this);
        else
            ((DHTDataNode.ReplyHandler) handler).onReceive(this);
    }

    public boolean isResult() {
        return result;
    }

}
