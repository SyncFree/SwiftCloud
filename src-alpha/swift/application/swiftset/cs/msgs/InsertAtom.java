package swift.application.swiftset.cs.msgs;

import swift.application.swiftdoc.TextLine;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

public class InsertAtom extends SwiftSetRpc {

    public TextLine atom;

    InsertAtom() {
    }

    public InsertAtom(TextLine atom, int pos) {
        this.atom = atom;
    }

    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        ((AppRpcHandler) handler).onReceive(handle, this);
    }

}
