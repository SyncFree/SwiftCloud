package sys.net.impl;

import sys.net.api.Endpoint;
import sys.net.api.Message;

public class FailedTransportConnection extends AbstractTransport {
	public FailedTransportConnection(Endpoint local, Endpoint remote) {
		super(local, remote);
		this.isBroken = true;
	}
	
    @Override
    public boolean send(Message m) {
    	return false;
    }

    @Override
    public <T extends Message> T receive() {
        return null;
    }
}