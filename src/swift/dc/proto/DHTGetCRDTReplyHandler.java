package swift.dc.proto;

import sys.dht.api.DHT;

public abstract class DHTGetCRDTReplyHandler extends DHT.AbstractReplyHandler {
    abstract public void onReceive(DHTGetCRDTReply reply);
}


