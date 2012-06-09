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
    public void deliverTo(DHT.Handle conn, DHT.ReplyHandler handler) {
        if (conn.expectingReply())
            ((DHTGetCRDTReplyHandler) handler).onReceive(conn, this);
        else
            ((DHTGetCRDTReplyHandler) handler).onReceive(this);
    }

    public CRDTObject getObject() {
        return object;
    }

}
