package swift.crdt;

import java.util.HashMap;
import java.util.Map.Entry;

import swift.clocks.TripleTimestamp;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class LWWStringMapRegisterUpdate extends LWWRegisterUpdate<HashMap<String, String>, LWWStringMapRegisterCRDT>
        implements KryoSerializable {
    // Kryo-use only
    public LWWStringMapRegisterUpdate() {
    }

    public LWWStringMapRegisterUpdate(long registerTimestamp, TripleTimestamp tiebreakingTimestamp,
            HashMap<String, String> val) {
        super(registerTimestamp, tiebreakingTimestamp, val);
    }

    @Override
    protected void writeValue(Kryo kryo, Output output) {
        output.writeVarInt(val.size(), true);
        for (Entry<String, String> entry : val.entrySet()) {
            output.writeString(entry.getKey());
            output.writeString(entry.getValue());
        }
    }

    @Override
    protected void readValue(Kryo kryo, Input input) {
        final int valSize = input.readVarInt(true);
        val = new HashMap<>(valSize);
        for (int i = 0; i < valSize; i++) {
            String key = input.readString();
            String value = input.readString();
            val.put(key, value);
        }
    }
}
