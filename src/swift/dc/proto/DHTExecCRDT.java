package swift.dc.proto;

import swift.client.proto.SubscriptionType;
import swift.clocks.CausalityClock;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDT;
import swift.crdt.operations.CRDTObjectOperationsGroup;
import swift.dc.*;
import sys.dht.api.DHT;

/**
 * Object for executing operations in a crdt
 * 
 * @author preguica
 */
public class DHTExecCRDT<V extends CRDT<V>> implements DHT.Message {

    CRDTObjectOperationsGroup<V> grp;
    CausalityClock snapshotVersion;
    CausalityClock trxVersion;
    String surrogateId;
    
    /**
     * Needed for Kryo serialization
     */
    public DHTExecCRDT() {
    }

    public DHTExecCRDT(String surrogateId, CRDTObjectOperationsGroup<V> grp, CausalityClock snapshotVersion, CausalityClock trxVersion) {
        this.surrogateId = surrogateId;
        this.grp = grp;
        this.snapshotVersion = snapshotVersion;
        this.trxVersion = trxVersion;
    }

    @Override
    public void deliverTo(DHT.Handle conn, DHT.Key key, DHT.MessageHandler handler) {
        ((DHTDataNode.RequestHandler) handler).onReceive(conn, key, this);
    }

    public String getSurrogateId() {
        return surrogateId;
    }

    public CRDTObjectOperationsGroup<V> getGrp() {
        return grp;
    }

    public CausalityClock getSnapshotVersion() {
        return snapshotVersion;
    }

    public CausalityClock getTrxVersion() {
        return trxVersion;
    }

}
