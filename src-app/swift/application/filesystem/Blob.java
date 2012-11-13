package swift.application.filesystem;

import swift.crdt.interfaces.Copyable;

public class Blob implements Copyable {
    private final byte[] content;

    public Blob() {
        this.content = new byte[0];
    }

    public Blob(byte[] s) {
        this.content = s;
    }

    public byte[] get() {
        return content;
    }

    @Override
    public Object copy() {
        return new Blob(this.content.clone());
    }

}
