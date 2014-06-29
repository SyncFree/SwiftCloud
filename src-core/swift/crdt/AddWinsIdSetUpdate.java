package swift.crdt;

import java.util.Set;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import swift.clocks.TripleTimestamp;
import swift.crdt.core.CRDTIdentifier;

public class AddWinsIdSetUpdate extends AddWinsSetUpdate<CRDTIdentifier, AddWinsIdSetCRDT> implements KryoSerializable {
    // Kryo-use only
    public AddWinsIdSetUpdate() {
    }

    public AddWinsIdSetUpdate(CRDTIdentifier val, TripleTimestamp newInstance, Set<TripleTimestamp> removedInstances) {
        super(val, newInstance, removedInstances);
    }

    @Override
    protected void writeElement(Kryo kryo, Output output) {
        val.write(kryo, output);
    }

    @Override
    protected void readElement(Kryo kryo, Input input) {
        val = new CRDTIdentifier();
        val.read(kryo, input);
    }
}
