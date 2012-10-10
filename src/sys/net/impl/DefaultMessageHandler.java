package sys.net.impl;

import sys.net.api.Endpoint;
import sys.net.api.Message;
import sys.net.api.MessageHandler;
import sys.net.api.TransportConnection;

public class DefaultMessageHandler implements MessageHandler {

    final boolean silent;

    public DefaultMessageHandler(){
        this(false);
    }
    
    public DefaultMessageHandler(boolean silent) {
        this.silent = silent;
    }

    @Override
    public void onAccept(TransportConnection conn) {
        if (!silent)
            Thread.dumpStack();
    }

    @Override
    public void onConnect(TransportConnection conn) {
        if (!silent)
            Thread.dumpStack();
    }

    @Override
    public void onFailure(TransportConnection conn) {
        if (!silent)
            Thread.dumpStack();
    }

    @Override
    public void onFailure(Endpoint dst, Message m) {
        if (!silent)
            Thread.dumpStack();
    }

    @Override
    public void onReceive(TransportConnection conn, Message m) {
        if (!silent)
            Thread.dumpStack();
    }

    @Override
    public void onClose(TransportConnection conn) {
        if (!silent)
            Thread.dumpStack();
    }
}
