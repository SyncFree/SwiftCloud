package sys.net.impl;

import sys.net.api.Endpoint;
import sys.net.api.Message;
import sys.net.api.NetworkingException;
import sys.net.api.TransportConnection;

public abstract class AbstractTransport implements TransportConnection {

    protected final Endpoint local;
    protected final Endpoint remote;
    protected boolean isBroken;
    
    public AbstractTransport(Endpoint local, Endpoint remote) {
        this.local = local;
        this.remote = remote;
        this.isBroken = false;
    }

    @Override
    public void dispose() {
    }

    @Override
    public boolean send(Message m) {
        throw new NetworkingException("Invalid connection state...");
    }

    @Override
    public <T extends Message> T receive() {
        throw new NetworkingException("Invalid connection state...");
    }

    @Override
    public boolean failed() {
    	return isBroken;
    }

    @Override
    public Endpoint localEndpoint() {
        return local;
    }

    @Override
    public Endpoint remoteEndpoint() {
        return remote;
    }
}