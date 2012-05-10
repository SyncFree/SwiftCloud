package swift.client.proto;

import swift.clocks.CausalityClock;
import swift.crdt.interfaces.CRDT;
import sys.net.api.rpc.RpcHandle;
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
         * The requested version of an object is available in the store.
         */
        VERSION_NOT_FOUND
    }

    protected FetchStatus status;
    // TODO: shalln't we use CRDT class simply and allow leaving certain fields
    // null?
    protected CRDT<?> crdt;
    protected CausalityClock version;
    protected CausalityClock pruneClock;
    protected CausalityClock estimatedLatestKnownClock;

    // Fake constructor for Kryo serialization. Do NOT use.
    public FetchObjectVersionReply() {
    }

    public FetchObjectVersionReply(FetchStatus status, CRDT<?> crdt, CausalityClock version, CausalityClock pruneClock,
            CausalityClock estimatedLatestKnownClock) {
        this.status = status;
        this.crdt = crdt;
        this.version = version;
        this.pruneClock = pruneClock;
        this.estimatedLatestKnownClock = estimatedLatestKnownClock;
    }

    /**
     * @return status code of the reply
     */
    public FetchStatus getStatus() {
        return status;
    }

    /**
     * @return state of an object requested by the client; null if
     *         {@link #getStatus()} is {@link FetchStatus#OBJECT_NOT_FOUND}.
     */
    // Old docs, not true anymore: if {@link #getStatus()} is {@link
    // FetchStatus#OK} then the object is
    // pruned from history at most to the level specified by version in
    // the original client request;
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
        return pruneClock;
    }

    /**
     * @return estimation of the latest committed clock in the store, dominated
     *         by or equals {@link #getVersion()}
     */
    public CausalityClock getEstimatedLatestKnownClock() {
        return estimatedLatestKnownClock;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((FetchObjectVersionReplyHandler) handler).onReceive(conn, this);
    }
}
