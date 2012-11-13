package swift.application.social.cs;

import sys.net.api.rpc.AbstractRpcHandler;
import sys.net.api.rpc.RpcHandle;

public abstract class RequestHandler extends AbstractRpcHandler {


		public void onReceive(final Request r) {
			Thread.dumpStack();
		}

		public void onReceive(final RpcHandle handle, final Request r) {
			Thread.dumpStack();
		}

}