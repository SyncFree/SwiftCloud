package swift.dc.proto;

import swift.dc.*;
import sys.dht.api.DHT;

/**
 * 
 * @author preguica
 * 
 */
public class DHTGetCRDTReply implements DHT.Reply {

    CRDTObject object;

    /**
     * Needed for Kryo serialization
     */
    public DHTGetCRDTReply() {
    }

    public DHTGetCRDTReply(CRDTObject object) {
        this.object = object;
    }

    @Override
    public void deliverTo(DHT.Connection conn, DHT.ReplyHandler handler) {
        if (conn.expectingReply())
            ((DHTDataNode.ReplyHandler) handler).onReceive(conn, this);
        else
            ((DHTDataNode.ReplyHandler) handler).onReceive(this);
    }

    public CRDTObject getObject() {
        return object;
    }

}
