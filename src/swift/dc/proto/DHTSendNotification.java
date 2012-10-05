package swift.dc.proto;

import swift.client.proto.SubscriptionType;
import swift.client.proto.FastRecentUpdatesReply.ObjectSubscriptionInfo;
import swift.clocks.CausalityClock;
import swift.crdt.CRDTIdentifier;
import swift.dc.*;
import sys.dht.api.DHT;
import sys.dht.api.DHT.Handle;
import sys.dht.api.DHT.ReplyHandler;

/**
 * Object for sending a notification of an update to a surrogate
 * 
 * @author preguica
 */
public class DHTSendNotification implements DHT.Reply {
    ObjectSubscriptionInfo info;
    CausalityClock estimatedDCVersion;
    CausalityClock estimatedDCStableVersion;
    
    /**
     * Needed for Kryo serialization
     */
    DHTSendNotification() {
    }

    public DHTSendNotification(ObjectSubscriptionInfo info, CausalityClock estimatedDCVersion, CausalityClock estimatedDCStableVersion) {
        super();
        this.info = info;
        this.estimatedDCVersion = estimatedDCVersion;
        this.estimatedDCStableVersion = estimatedDCStableVersion;
    }


    public ObjectSubscriptionInfo getInfo() {
        return info;
    }

    @Override
    public void deliverTo(Handle conn, ReplyHandler handler) {
        ((DHTDataNode.ReplyHandler) handler).onReceive(conn, this);
        
    }

    public CausalityClock getEstimatedDCVersion() {
        return estimatedDCVersion;
    }

    public CausalityClock getEstimatedDCStableVersion() {
        return estimatedDCStableVersion;
    }

}
