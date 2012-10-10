package sys.net.impl.rpc;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import sys.net.api.MessageHandler;
import sys.net.api.TransportConnection;

public class RpcPing extends RpcEcho  implements KryoSerializable{
	
	public long departure_ts;

	//for kryo
	public RpcPing() {			    
	}
			
	@Override
	public void deliverTo(TransportConnection conn, MessageHandler handler) {
		((RpcEchoHandler) handler).onReceive(conn, this);
	}

    @Override
    public void read(Kryo kryo, Input in) {
        this.departure_ts = in.readLong();
    }

    @Override
    public void write(Kryo kryo, Output out) {
        out.writeLong( departure_ts = System.currentTimeMillis() );
    }	
}
