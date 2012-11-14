/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
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
