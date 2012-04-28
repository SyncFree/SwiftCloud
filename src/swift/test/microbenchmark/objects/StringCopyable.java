package swift.test.microbenchmark.objects;

import swift.crdt.interfaces.Copyable;

public class StringCopyable implements Copyable {

    private String value;

    public StringCopyable() {

    }

    public StringCopyable(String value) {
        this.value = value;
    }

    public Object copy() {
        return new String(value);
    }

}
