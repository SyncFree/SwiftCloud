package sys.net.impl.rpc;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import sys.net.api.MessageHandler;
import sys.net.api.TransportConnection;

public class RpcPong extends RpcEcho implements KryoSerializable {
	
	public long departure_ts;
	public long arrival_ts;
	
	public RpcPong() {		
	}
	
	public RpcPong( RpcPing other) {
		this.departure_ts = other.departure_ts;
	}
		
	public double rtt() {
		return arrival_ts - departure_ts;
	}
	
	@Override
	public void deliverTo(TransportConnection conn, MessageHandler handler) {
		((RpcEchoHandler) handler).onReceive(conn, this);
	}

    @Override
    public void read(Kryo kryo, Input in) {
        departure_ts = in.readLong();
        arrival_ts = System.currentTimeMillis();
    }
  
    @Override
    public void write(Kryo kryo, Output out) {
        out.writeLong( departure_ts );
    }	
}
