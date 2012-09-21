package swift.application.social.cdn;

import sys.net.api.rpc.AbstractRpcHandler;
import sys.net.api.rpc.RpcHandle;

public abstract class CdnRpcHandler extends AbstractRpcHandler {


		public void onReceive(final CdnRpc r) {
			Thread.dumpStack();
		}

		public void onReceive(final RpcHandle handle, final CdnRpc r) {
			Thread.dumpStack();
		}

}