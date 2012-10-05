package swift.application.filesystem;

import swift.crdt.interfaces.Copyable;

public class StringCopyable implements Copyable {
    private final String content;

    public StringCopyable() {
        this.content = "";
    }

    public StringCopyable(String s) {
        this.content = s;
    }

    public String getString() {
        return content;
    }

    @Override
    public Object copy() {
        return new StringCopyable(this.content);
    }

}
