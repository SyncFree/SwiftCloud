package swift.dc.proto;

import swift.client.proto.SubscriptionType;
import swift.clocks.CausalityClock;
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
    CausalityClock version;
    
    /**
     * Needed for Kryo serialization
     */
    DHTGetCRDT() {
    }

    public DHTGetCRDT(String surrogateId, CRDTIdentifier id, SubscriptionType subscribe, CausalityClock version) {
        super();
        this.surrogateId = surrogateId;
        this.id = id;
        this.subscribe = subscribe;
        this.version = version;
    }

    @Override
    public void deliverTo(DHT.Handle conn, DHT.Key key, DHT.MessageHandler handler) {
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

    public CausalityClock getVersion() {
        return version;
    }
}
