package swift.dc.proto;

import swift.client.proto.SubscriptionType;
import swift.crdt.CRDTIdentifier;
import swift.dc.*;
import sys.dht.api.DHT;

/**
 * Object for getting a crdt
 * 
 * @author preguica
 */
public class DHTGetCRDT implements DHT.Message {

    String surrogateId;
    CRDTIdentifier id;
    SubscriptionType subscribe;
    
    /**
     * Needed for Kryo serialization
     */
    public DHTGetCRDT() {
    }

    public DHTGetCRDT(String surrogateId, CRDTIdentifier id, SubscriptionType subscribe) {
        super();
        this.surrogateId = surrogateId;
        this.id = id;
        this.subscribe = subscribe;
    }

    @Override
    public void deliverTo(DHT.Connection conn, DHT.Key key, DHT.MessageHandler handler) {
        ((DHTDataNode.RequestHandler) handler).onReceive(conn, key, this);
    }

    public String getSurrogateId() {
        return surrogateId;
    }

    public CRDTIdentifier getId() {
        return id;
    }

    public SubscriptionType getSubscribe() {
        return subscribe;
    }
}