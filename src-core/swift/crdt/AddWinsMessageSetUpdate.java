package swift.crdt;

import java.util.Set;

import swift.application.social.Message;
import swift.clocks.TripleTimestamp;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class AddWinsMessageSetUpdate extends AddWinsSetUpdate<Message, AddWinsMessageSetCRDT> implements
        KryoSerializable {
    // Kryo-use only
    public AddWinsMessageSetUpdate() {
    }

    public AddWinsMessageSetUpdate(Message val, TripleTimestamp newInstance, Set<TripleTimestamp> removedInstances) {
        super(val, newInstance, removedInstances);
    }

    @Override
    protected void writeElement(Kryo kryo, Output output) {
        val.write(kryo, output);
    }

    @Override
    protected void readElement(Kryo kryo, Input input) {
        val = new Message();
        val.read(kryo, input);
    }

}
