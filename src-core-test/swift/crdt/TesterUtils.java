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
package swift.crdt;

import swift.clocks.IncrementalTimestampGenerator;
import swift.clocks.IncrementalTripleTimestampGenerator;
import swift.clocks.Timestamp;
import swift.clocks.TimestampMapping;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.TxnLocalCRDT;

public class TesterUtils {

    public static void printInformtion(CRDT<?> i, TxnTester txn) {
        System.out.println(i.getClock());
        System.out.println(i.toString());

        System.out.println(txn.getClock());
        System.out.println(getTxnLocal(i, txn).getValue());
    }

    @SuppressWarnings("unchecked")
    public static <V extends CRDT<V>> TxnLocalCRDT<V> getTxnLocal(CRDT<V> i, TxnTester txn) {
        return i.getTxnLocalCopy(i.getClock(), txn);
    }

    public static TripleTimestamp generateTripleTimestamp(String site, int counter, int secondaryCounter) {
        final IncrementalTimestampGenerator tsGenerator = new IncrementalTimestampGenerator(site);
        Timestamp timestamp = null;
        for (int i = 0; i < counter; i++) {
            timestamp = tsGenerator.generateNew();
        }
        final IncrementalTripleTimestampGenerator ttsGenerator = new IncrementalTripleTimestampGenerator(
                new TimestampMapping(timestamp));

        TripleTimestamp tripleTimestamp = null;
        for (int i = 0; i < secondaryCounter; i++) {
            tripleTimestamp = ttsGenerator.generateNew();
        }
        return tripleTimestamp;
    }
}
