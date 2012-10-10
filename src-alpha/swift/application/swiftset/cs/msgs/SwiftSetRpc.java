package swift.application.swiftset.cs.msgs;

import java.util.concurrent.atomic.AtomicLong;

import sys.net.api.rpc.RpcMessage;

public abstract class SwiftSetRpc implements RpcMessage{

    static AtomicLong serialFactory = new AtomicLong(0);
    
    public long serial = serialFactory.getAndIncrement() ;
                
}
