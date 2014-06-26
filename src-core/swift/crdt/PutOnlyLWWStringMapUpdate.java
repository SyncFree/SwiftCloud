package swift.crdt;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import swift.clocks.TripleTimestamp;

public class PutOnlyLWWStringMapUpdate extends AbstractPutOnlyLWWMapUpdate<PutOnlyLWWStringMapCRDT, String, String> implements
        KryoSerializable {

    public PutOnlyLWWStringMapUpdate() {
        // Kryo-use only
    }

    public PutOnlyLWWStringMapUpdate(String key, long timestamp, TripleTimestamp timestampTiebreaker, String val) {
        super(key, timestamp, timestampTiebreaker, val);
    }

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeString(key);
        output.writeString(val);
        output.writeLong(registerTimestamp);
        tiebreakingTimestamp.write(kryo, output);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        key = input.readString();
        val = input.readString();
        registerTimestamp = input.readLong();
        tiebreakingTimestamp = new TripleTimestamp();
        tiebreakingTimestamp.read(kryo, input);
    }
}
