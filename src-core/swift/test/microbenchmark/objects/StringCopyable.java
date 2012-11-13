package swift.test.microbenchmark.objects;

import swift.crdt.interfaces.Copyable;

public class StringCopyable implements Copyable {

    private String value;

    public StringCopyable() {

    }

    public StringCopyable(String value) {
        this.value = value;
    }

    @Override
    public Object copy() {
        return new StringCopyable(value);
    }
    public String getValue(){
        return value;
    }
    @Override
    public String toString() {
        return value;
    }
    

}
