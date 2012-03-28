package swift.test.crdt;

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

}
