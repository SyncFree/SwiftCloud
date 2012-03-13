package swift.crdt;

import swift.clocks.CausalityClock;

public final class SetIntegers extends SetVersioned<Integer, SetIntegers> {
    private static final long serialVersionUID = 1L;

    public SetIntegers() {
        super();
    }

}
