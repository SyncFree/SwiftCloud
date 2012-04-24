package swift.dc.proto;

import sys.dht.api.DHT;

public abstract class DHTExecCRDTReplyHandler extends DHT.AbstractReplyHandler {
    abstract public void onReceive(DHTExecCRDTReply reply);
}


