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
    public enum FetchStatus {
        /**
         * The reply contains requested version.
         */
        OK,
        /**
         * The requested object is not in the store.
         */
        OBJECT_NOT_FOUND,
        /**
         * The requested version of an object is not any more available in the
         * store.
         */
        VERSION_NOT_FOUND
    }

    protected FetchStatus status;
    // TODO: shalln't we use CRDT class simply and allow leaving certain fields
    // null?
    protected CRDT<?> crdt;
    protected CausalityClock version;
    protected CausalityClock pruneClock;

    // Fake constructor for Kryo serialization. Do NOT use.
    public FetchObjectVersionReply() {
    }

    public FetchObjectVersionReply(FetchStatus status, CRDT<?> crdt, CausalityClock version, CausalityClock pruneClock) {
        this.status = status;
        this.crdt = crdt;
        this.version = version;
        this.pruneClock = pruneClock;
    }

    /**
     * @return status code of the reply
     */
    public FetchStatus getStatus() {
        return status;
    }

    /**
     * @return state of an object requested by the client; if
     *         {@link #getStatus()} is {@link FetchStatus#OK} then the object is
     *         pruned from history at most to the level specified by version in
     *         the original client request; null if {@link #getStatus()} is
     *         {@link FetchStatus#OBJECT_NOT_FOUND}.
     */
    public CRDT<?> getCrdt() {
        return crdt;
    }

    /**
     * @return version of an object returned, possibly higher than the version
     *         requested by the client; if {@link #getStatus()} is
     *         {@link FetchStatus#OBJECT_NOT_FOUND} then it is the latest clock
     *         known when object does not exist
     */
    public CausalityClock getVersion() {
        return version;
    }

    /**
     * @return pruneClock of an object returned; null if {@link #getStatus()} is
     *         {@link FetchStatus#OBJECT_NOT_FOUND}
     */
    public CausalityClock getPruneClock() {
        return version;
    }

    @Override
    public void deliverTo(RpcConnection conn, RpcHandler handler) {
        ((FetchObjectVersionReplyHandler) handler).onReceive(conn, this);
    }
}
