package swift.application.ycsb;

import java.util.HashMap;

import swift.crdt.core.Copyable;

/**
 * Copyable wrapper of a String-to-String Hashmap. Workaround to use HashMap
 * with LWW register {@link RegisterVersioned}.
 * 
 * @author mzawirsk
 */
public class StringHashMapWrapper implements Copyable {
    /**
     * 
     */
    private static final long serialVersionUID = 3583391796205883150L;

    public static StringHashMapWrapper createWithValue(HashMap<String, String> value) {
        final StringHashMapWrapper wrapper = new StringHashMapWrapper();
        wrapper.map = value;
        return wrapper;
    }

    private HashMap<String, String> map;

    /**
     * Kryo-only, DO NOT USE. Use {@link #createWithValue(HashMap)}
     */
    public StringHashMapWrapper() {
    }

    public HashMap<String, String> getValue() {
        return map;
    }

    @Override
    public Object copy() {
        final StringHashMapWrapper wrapper = new StringHashMapWrapper();
        wrapper.map = new HashMap<String, String>(this.map);
        return wrapper;
    }
}
