package swift.crdt;

import java.util.HashSet;
import java.util.Set;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import swift.clocks.CausalityClock;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;

/**
 * An add-only set optimized for storing Strings.
 * 
 * @author mzawirsk
 */
public class AddOnlyStringSetCRDT extends AbstractAddOnlySetCRDT<AddOnlyStringSetCRDT, String> implements
        KryoSerializable {
    // Kryo
    public AddOnlyStringSetCRDT() {
    }

    public AddOnlyStringSetCRDT(CRDTIdentifier id) {
        super(id);
    }

    private AddOnlyStringSetCRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, Set<String> elements) {
        super(id, txn, clock, elements);
    }

    @Override
    protected AddOnlySetUpdate<AddOnlyStringSetCRDT, String> generateAddDownstream(String element) {
        return new AddOnlyStringSetUpdate(element);
    }

    @Override
    public AddOnlyStringSetCRDT copy() {
        return new AddOnlyStringSetCRDT(id, txn, clock, new HashSet<String>(elements));
    }

    @Override
    public void write(Kryo kryo, Output output) {
        baseWrite(kryo, output);
        output.writeVarInt(elements.size(), true);
        for (final String element : elements) {
            output.writeString(element);
        }
    }

    @Override
    public void read(Kryo kryo, Input input) {
        baseRead(kryo, input);
        final int elementsSize = input.readVarInt(true);
        elements = new HashSet<String>(elementsSize);
        for (int i = 0; i < elementsSize; i++) {
            elements.add(input.readString());
        }
    }
}
