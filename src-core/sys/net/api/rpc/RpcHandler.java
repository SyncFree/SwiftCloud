package sys.net.api.rpc;

/**
 * 
 * 
 * @author smd
 * 
 */
public interface RpcHandler {

	void onReceive(final RpcMessage m);

	void onReceive(final RpcHandle handle, final RpcMessage m);

	void onFailure(final RpcHandle handle);

	static public final RpcHandler NONE = new RpcHandler() {

		public void onReceive(final RpcMessage m) {
		}

		public void onFailure(final RpcHandle handle) {
		}

		public void onReceive(final RpcHandle handle, final RpcMessage m) {
		}

	};
}
