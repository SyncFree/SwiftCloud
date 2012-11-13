package sys.net.impl;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

import static sys.Sys.*;
/**
 * 
 * @author smd
 * 
 */
public class Request implements RpcMessage, KryoSerializable {

    public int val;
    public double timestamp;
    
    Request() {
    }

    public Request(int val) {
        this.val = val;
        this.timestamp = Sys.currentTime();
    }

    public Request(int val, double ts ) {
        this.val = val;
        this.timestamp = ts;
    }
    
    public String toString() {
        return "request: " + val;
    }

    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        ((Handler) handler).onReceive(handle, this);
    }

	@Override
	final public void read(Kryo kryo, Input input) {
		this.val = input.readInt();
		this.timestamp = input.readDouble();
	}

	@Override
	final public void write(Kryo kryo, Output output) {
		output.writeInt( this.val ) ;
		output.writeDouble( this.timestamp);
	}
}
