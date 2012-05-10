package sys.benchmarks.rpc;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

import static sys.Sys.*;
/**
 * 
 * @author smd
 * 
 */
public class Request implements RpcMessage {

    public int val;
    public double timestamp = Sys.currentTime();
    
    Request() {
    }

    public Request(int val) {
        this.val = val;
    }

    public String toString() {
        return "request: " + val;
    }

    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        ((Handler) handler).onReceive(handle, this);
    }
}
