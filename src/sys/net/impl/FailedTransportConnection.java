package sys.net.impl;

import sys.net.api.Endpoint;
import sys.net.api.Message;

public class FailedTransportConnection extends AbstractTransport {

	Throwable cause;

	public FailedTransportConnection(Endpoint local, Endpoint remote, Throwable cause) {
		super(local, remote);
		this.isBroken = true;
		this.cause = cause;
	}

	@Override
	public boolean send(Message m) {
		return false;
	}

	@Override
	public <T extends Message> T receive() {
		return null;
	}

	public Throwable causeOfFailure() {
		return cause;
	}
}