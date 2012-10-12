package swift.utils;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public final class PrettyPrint {
    private PrettyPrint() {
    }

    public static <K, V> String printMap(String start, String end, String sep, String map, Map<K, V> elems) {
        StringBuffer buf = new StringBuffer();
        buf.append(start);
        Iterator<Entry<K, V>> it = elems.entrySet().iterator();
        while (it.hasNext()) {
            Entry<K, V> e = it.next();
            buf.append(e.getKey());
            buf.append(map);
            buf.append(e.getValue());
            if (it.hasNext()) {
                buf.append(sep);
            }
        }
        buf.append(end);
        return buf.toString();
    }

    public static <K, V> String printCollection(String start, String end, String sep, Collection<V> elems) {
        StringBuffer buf = new StringBuffer();
        buf.append(start);
        Iterator<V> it = elems.iterator();
        while (it.hasNext()) {
            V e = it.next();
            buf.append(e);
            if (it.hasNext()) {
                buf.append(sep);
            }
        }
        buf.append(end);
        return buf.toString();
    }
}
