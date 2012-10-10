package swift.application.swiftdoc.cs.msgs;

import sys.net.api.rpc.AbstractRpcHandler;
import sys.net.api.rpc.RpcHandle;

public abstract class AppRpcHandler extends AbstractRpcHandler {

    public void onReceive(final RpcHandle client, final InitScoutServer r) {
        Thread.dumpStack();
    }

    public void onReceive(final RpcHandle client, final BeginTransaction r) {
        Thread.dumpStack();
    }

    public void onReceive(final RpcHandle client, final CommitTransaction r) {
        Thread.dumpStack();
    }

    public void onReceive(final RpcHandle client, final InsertAtom r) {
        Thread.dumpStack();
    }

    public void onReceive(final RpcHandle client, final RemoveAtom r) {
        Thread.dumpStack();
    }

    public void onReceive(final RpcHandle client, final BulkTransaction r) {
        Thread.dumpStack();
    }

    public void onReceive(final ServerReply r) {
        Thread.dumpStack();
    }

    public void onReceive(final ServerACK r) {
    }
}