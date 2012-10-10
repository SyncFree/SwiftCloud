package swift.application.swiftset;

import java.util.concurrent.atomic.AtomicLong;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;


public class TextLine implements Comparable<TextLine>, KryoSerializable {

    static AtomicLong g_serial = new AtomicLong(0);

    long serial;
    String text;
    long arrival_ts;
    long departure_ts;

    boolean warmup;

    // for kryo
    TextLine() {
    }

    public TextLine(String text) {
        this(text, false);
    }

    public TextLine(String text, boolean warmup) {
        this.text = text;
        this.warmup = warmup;
        this.serial = g_serial.getAndIncrement();
        this.departure_ts = -1;
    }

    public boolean isWarmUp() {
        return warmup;
    }
    
    public int hashCode() {
        return text.hashCode();
    }

    public boolean equals(Object other) {
        if (other instanceof String)
            return text.equals(other);
        else
            return other instanceof TextLine && text.equals(((TextLine) other).text);
    }

    @Override
    public void read(Kryo kryo, Input in) {
        serial = in.readLong();
        text = in.readString();
        warmup = in.readBoolean();
        departure_ts = in.readLong();
        arrival_ts = System.currentTimeMillis();
    }

    @Override
    public void write(Kryo kryo, Output out) {
        out.writeLong(serial);
        out.writeString(text);
        out.writeBoolean(warmup);
        out.writeLong(departure_ts >= 0 ? departure_ts : (departure_ts = System.currentTimeMillis()));
    }

    public long latency() {
        return (arrival_ts - departure_ts) / 2;
    }

    public String toString() {
        return text;
    }

    public Long serial() {
        return serial;
    }

    @Override
    public int compareTo(TextLine other) {
        return text.compareTo( other.text ) ;
    }
}
