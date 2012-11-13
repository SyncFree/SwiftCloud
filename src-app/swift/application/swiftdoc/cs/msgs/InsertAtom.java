package swift.application.swiftdoc.cs.msgs;

import swift.application.swiftdoc.TextLine;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

public class InsertAtom extends SwiftDocRpc {

    public int pos;
    public TextLine atom;

    InsertAtom() {
    }

    public InsertAtom(TextLine atom, int pos) {
        this.pos = pos;
        this.atom = atom;
    }

    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        ((AppRpcHandler) handler).onReceive(handle, this);
    }

}
