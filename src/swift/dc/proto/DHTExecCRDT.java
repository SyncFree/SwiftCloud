package swift.dc.proto;

import swift.client.proto.SubscriptionType;
import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDT;
import swift.crdt.operations.CRDTObjectUpdatesGroup;
import swift.dc.*;
import sys.dht.api.DHT;

/**
 * Object for executing operations in a crdt
 * 
 * @author preguica
 */
public class DHTExecCRDT<V extends CRDT<V>> implements DHT.Message {

    CRDTObjectUpdatesGroup<V> grp;
    CausalityClock snapshotVersion;
    CausalityClock trxVersion;
    Timestamp txTs;
    Timestamp cltTs;
    Timestamp prvCltTs;
    String surrogateId;
    
    /**
     * Needed for Kryo serialization
     */
    DHTExecCRDT() {
    }

    public DHTExecCRDT(String surrogateId, CRDTObjectUpdatesGroup<V> grp, CausalityClock snapshotVersion, 
            CausalityClock trxVersion, Timestamp txTs, Timestamp cltTs, Timestamp prvCltTs) {
        this.surrogateId = surrogateId;
        this.grp = grp;
        this.snapshotVersion = snapshotVersion;
        this.trxVersion = trxVersion;
        this.txTs = txTs;
        this.cltTs = cltTs;
        this.prvCltTs = prvCltTs;
    }

    @Override
    public void deliverTo(DHT.Handle conn, DHT.Key key, DHT.MessageHandler handler) {
        ((DHTDataNode.RequestHandler) handler).onReceive(conn, key, this);
    }

    public String getSurrogateId() {
        return surrogateId;
    }

    public CRDTObjectUpdatesGroup<V> getGrp() {
        return grp;
    }

    public CausalityClock getSnapshotVersion() {
        return snapshotVersion;
    }

    public CausalityClock getTrxVersion() {
        return trxVersion;
    }

    public Timestamp getTxTs() {
        return txTs;
    }

    public Timestamp getCltTs() {
        return cltTs;
    }

    public Timestamp getPrvCltTs() {
        return prvCltTs;
    }

}
