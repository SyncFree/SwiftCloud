package swift.client.proto;

import swift.clocks.CausalityClock;
import swift.crdt.interfaces.CRDT;
import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * Server reply to object version fetch request.
 * 
 * @author mzawirski
 */
public class FetchObjectVersionReply implements RpcMessage {
    // TODO: shalln't we use CRDT class simply and allow leaving certain fields
    // null?
    protected CRDT<?> crdt;
    protected CausalityClock version;

    // Fake constructor for Kryo serialization. Do NOT use.
    public FetchObjectVersionReply() {
    }

    public FetchObjectVersionReply(CRDT<?> crdt, CausalityClock version) {
        this.crdt = crdt;
        this.version = version;
    }

    /**
     * @return state of an object requested by the client, pruned from history
     *         at most to the level specified by version in the original client
     *         request; null if {@link #isFound()} returns false
     */
    public CRDT<?> getCrdt() {
        return crdt;
    }

    /**
     * @return version of an object returned, possibly higher than the version
     *         requested by the client; if {@link #isFound()} returns true, this
     *         represents the latest clock known when object was absent in the
     *         store
     */
    public CausalityClock getVersion() {
        return version;
    }

    /**
     * @return true if object was found in the store, false otherwise
     */
    public boolean isFound() {
        return crdt == null;
    }

    @Override
    public void deliverTo(RpcConnection conn, RpcHandler handler) {
        ((FetchObjectVersionReplyHandler) handler).onReceive(conn, this);
    }
}
