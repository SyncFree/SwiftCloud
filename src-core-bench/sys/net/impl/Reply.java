package sys.net.impl;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

import static sys.Sys.*;

import sys.net.api.*;
/**
 * 
 * @author smd
 * 
 */
public class Reply implements RpcMessage, KryoSerializable {

    public int val;
    public double timestamp;
    
	public Reply() {
	}

	public Reply( int val, double ts) {
	    this.val = val;
	    this.timestamp = ts;
    }

	double rtt() {
		return (Sys.currentTime() - timestamp) * 1000000;
	}
	
	public String toString() {
        return "reply " + val;
	}
	
	@Override
	public void deliverTo( RpcHandle handle, RpcHandler handler) {
		if (handle.expectingReply())
			((Handler) handler).onReceive(handle, this);
		else
			((Handler) handler).onReceive(this);
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
