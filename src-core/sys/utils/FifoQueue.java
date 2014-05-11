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
package sys.utils;

import java.util.SortedMap;
import java.util.TreeMap;

public class FifoQueue<T> {
    Long nextKey = 1L;

    SortedMap<Long, T> queue = new TreeMap<Long, T>();

    synchronized public void offer(long seqN, T val) {
        queue.put(seqN, val);

        Long headKey;
        while (queue.size() > 0 && (headKey = queue.firstKey()).longValue() == nextKey) {
            process(queue.remove(headKey));
            nextKey++;
        }
    }

    public void process(T val) {
    }

    synchronized public String toString() {
        return nextKey + ":" + queue.keySet().toString();
    }
}
