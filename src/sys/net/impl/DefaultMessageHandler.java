package sys.net.impl;

import sys.net.api.Endpoint;
import sys.net.api.Message;
import sys.net.api.MessageHandler;
import sys.net.api.TransportConnection;

public class DefaultMessageHandler implements MessageHandler {

	@Override
	public void onAccept(TransportConnection conn) {
		Thread.dumpStack();
	}

	@Override
	public void onConnect(TransportConnection conn) {
		Thread.dumpStack();
	}

	@Override
	public void onFailure(TransportConnection conn) {
		Thread.dumpStack();
	}

	@Override
	public void onFailure(Endpoint dst, Message m) {
		Thread.dumpStack();
	}

	@Override
	public void onReceive(TransportConnection conn, Message m) {
		Thread.dumpStack();
	}

	@Override
	public void onClose(TransportConnection conn) {
		Thread.dumpStack();
	}
}
