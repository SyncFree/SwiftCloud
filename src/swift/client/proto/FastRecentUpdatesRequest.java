package swift.client.proto;

import swift.clocks.CausalityClock;
import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;

/**
 * Client request for recent updates on previously subscribed objects (see
 * {@link FetchObjectVersionRequest#isSubscribeUpdatesRequest()}. Beside of
 * requesting recent updates, this request keep client subscriptions alive.
 * <p>
 * This call may block until new updates arrive, a timeout elapses on the server
 * (to inform of no updates) or server confirms a new subscription. Client
 * should repeatedly send this request in order to get new updates and keep her
 * subscriptions alive.
 * 
 * @author nmp
 */
public class FastRecentUpdatesRequest extends ClientRequest {
    public enum SubscriptionType {
        /**
         * Receive updates on changes.
         */
        UPDATES,
        /**
         * Receive a single notification on changes.
         */
        NOTIFICATION,
        /**
         * Receive nothing on changes.
         */
        NONE
    }
   /**
     * No-args constructor for Kryo-serialization.
     */
    public FastRecentUpdatesRequest() {
    }

    public FastRecentUpdatesRequest(final String clientId) {
        super(clientId);
    }

    @Override
    public void deliverTo(RpcConnection conn, RpcHandler handler) {
        ((SwiftServer) handler).onReceive(conn, this);
    }
}
