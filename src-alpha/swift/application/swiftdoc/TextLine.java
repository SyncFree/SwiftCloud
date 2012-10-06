package swift.application.swiftdoc;

import java.util.concurrent.atomic.AtomicLong;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class TextLine implements KryoSerializable {

	static AtomicLong g_serial = new AtomicLong(0);
	
	long serial = g_serial.getAndIncrement();
	
	String text;
	long arrival_ts = -1;
	long departure_ts = -1;

	// for kryo
	
	TextLine(){		
	}
	
	TextLine(String text) {
		this.text = text;
	}

//	void markArrival() {
//		this.arrival_ts = NtpTime.timeInMillis();
//	}
	
	public int hashCode() {
		return text.hashCode();
	}
	
	public boolean equals( Object other ) {
		if( other instanceof String)
			return text.equals( other );
		else
			return  other instanceof TextLine && text.equals( ((TextLine)other).text);
	}

	@Override
	public void read(Kryo kryo, Input in) {
		text = in.readString();
		departure_ts = in.readLong();
		arrival_ts = NtpTime.timeInMillis();
	}

	@Override
	public void write(Kryo kryo, Output out) {
		out.writeString( text ) ;
		out.writeLong( departure_ts >= 0 ? departure_ts : NtpTime.timeInMillis() ) ;
	}
	
	public long latency() {
		return arrival_ts - departure_ts ;
	}
	
	public String toString() {
		return text;
	}
	
	public Long serial() {
		return serial;
	}
}
