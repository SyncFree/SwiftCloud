package swift.crdt;

import swift.clocks.TripleTimestamp;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class LWWStringRegisterUpdate extends LWWRegisterUpdate<String, LWWStringRegisterCRDT> implements
        KryoSerializable {
    // Kryo-use only
    public LWWStringRegisterUpdate() {
    }

    public LWWStringRegisterUpdate(long registerTimestamp, TripleTimestamp tiebreakingTimestamp, String val) {
        super(registerTimestamp, tiebreakingTimestamp, val);
    }

    @Override
    protected void writeValue(Kryo kryo, Output output) {
        output.writeString(val);
    }

    @Override
    protected void readValue(Kryo kryo, Input input) {
        val = input.readString();
    }
}
