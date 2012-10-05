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
