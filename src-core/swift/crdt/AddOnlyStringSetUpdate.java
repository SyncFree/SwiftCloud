package swift.crdt;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class AddOnlyStringSetUpdate extends AddOnlySetUpdate<AddOnlyStringSetCRDT, String> implements KryoSerializable {
    // Kryo
    public AddOnlyStringSetUpdate() {
    }

    public AddOnlyStringSetUpdate(String element) {
        super(element);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        element = input.readString();
    }

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeString(element);
    }
}
