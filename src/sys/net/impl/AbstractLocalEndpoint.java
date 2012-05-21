package sys.net.impl;

import sys.net.api.Endpoint;
import sys.net.api.Message;
import sys.net.api.TransportConnection;

public abstract class AbstractLocalEndpoint extends AbstractEndpoint {

	protected int tcpPort;
	protected Endpoint localEndpoint;

	abstract public void start() throws Exception ;
	
	abstract public TransportConnection connect(Endpoint dst) ;

	public abstract int getLocalPort() ;
	
	@Override
	public TransportConnection send(Endpoint remote, Message m) {
		TransportConnection conn = connect(remote);
		if (conn != null && conn.send(m))
			return conn;
		else
			return null;
	}

}
