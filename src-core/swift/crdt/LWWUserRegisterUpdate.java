package swift.crdt;

import swift.application.social.User;
import swift.clocks.TripleTimestamp;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class LWWUserRegisterUpdate extends LWWRegisterUpdate<User, LWWUserRegisterCRDT> implements KryoSerializable {
    // Kryo-use only
    public LWWUserRegisterUpdate() {
    }

    public LWWUserRegisterUpdate(long registerTimestamp, TripleTimestamp tiebreakingTimestamp, User val) {
        super(registerTimestamp, tiebreakingTimestamp, val);
    }

    @Override
    protected void writeValue(Kryo kryo, Output output) {
        val.write(kryo, output);
    }

    @Override
    protected void readValue(Kryo kryo, Input input) {
        val = new User();
        val.read(kryo, input);
    }
}
