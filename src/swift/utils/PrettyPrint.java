package swift.utils;

import java.util.Iterator;
import java.util.Set;

public final class PrettyPrint {
    private PrettyPrint() {
    }

    public static String printSet(String start, String end, String sep, Set<?> elems) {
        StringBuffer buf = new StringBuffer();
        buf.append(start);
        Iterator<?> it = elems.iterator();
        while (it.hasNext()) {
            Object e = it.next();
            buf.append(e);
            if (it.hasNext()) {
                buf.append(sep);
            }
        }
        buf.append(end);
        return buf.toString();

    }
}
