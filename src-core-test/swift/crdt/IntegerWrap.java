package swift.crdt;

import swift.crdt.interfaces.Copyable;

public class IntegerWrap implements Copyable {
    public final Integer i;

    public IntegerWrap(Integer i) {
        this.i = i;
    }

    @Override
    public Object copy() {
        return i;
    }

    @Override
    public String toString() {
        return i.toString();
    }

}
