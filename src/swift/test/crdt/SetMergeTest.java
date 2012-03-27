package swift.test.crdt;

import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import swift.clocks.ClockFactory;
import swift.clocks.TripleTimestamp;
import swift.crdt.SetIntegers;
import swift.crdt.SetTxnLocalInteger;
import swift.crdt.operations.SetInsert;
import swift.crdt.operations.SetRemove;
import swift.exceptions.ConsistentSnapshotVersionNotFoundException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.WrongTypeException;

public class SetMergeTest {
    TxnTester txn1, txn2;
    SetIntegers i1, i2;

    private void merge() {
        i1.merge(i2);
        txn1.updateClock(txn2.getClock());
    }

    private void printInformtion(SetIntegers i, TxnTester txn) {
        System.out.println(i.getClock());
        System.out.println(txn.getClock());
        System.out.println(getTxnLocal(i, txn).getValue());
    }

    private TripleTimestamp registerInsert(int value, SetIntegers i, TxnTester txn) {
        TripleTimestamp ts = txn.nextTimestamp();
        txn1.registerOperation(i, new SetInsert<Integer, SetIntegers>(ts, value));
        return ts;
    }

    private void registerRemove(int value, Set<TripleTimestamp> rems, SetIntegers i,
            TxnTester txn) {
        txn1.registerOperation(i, new SetRemove<Integer, SetIntegers>(txn.nextTimestamp(), value, rems));
    }

    private SetTxnLocalInteger getTxnLocal(SetIntegers i, TxnTester txn) {
        return (SetTxnLocalInteger) i.getTxnLocalCopy(i.getClock(), txn);
    }

    @Before
    public void setUp() throws WrongTypeException, NoSuchObjectException, ConsistentSnapshotVersionNotFoundException {
        i1 = new SetIntegers();
        i1.setClock(ClockFactory.newClock());
        i1.setPruneClock(ClockFactory.newClock());

        i2 = new SetIntegers();
        i2.setClock(ClockFactory.newClock());
        i2.setPruneClock(ClockFactory.newClock());
        txn1 = new TxnTester("client1", ClockFactory.newClock());
        txn2 = new TxnTester("client2", ClockFactory.newClock());
    }

    // Merge with empty set
    @Test
    public void mergeEmpty1() {
        registerInsert(5, i1, txn1);
        printInformtion(i1, txn1);

        merge();
        printInformtion(i1, txn1);
        assertTrue(getTxnLocal(i1, txn1).lookup(5));
    }

    // Merge with empty set
    @Test
    public void mergeEmpty2() {
        registerInsert(5, i2, txn2);
        merge();
        assertTrue(getTxnLocal(i1, txn1).lookup(5));
    }

    @Test
    public void mergeNonEmpty() {
        registerInsert(5, i1, txn1);
        registerInsert(6, i2, txn2);
        merge();
        assertTrue(getTxnLocal(i1, txn1).lookup(5));
        assertTrue(getTxnLocal(i1, txn1).lookup(6));
    }

    @Test
    public void mergeConcurrentInsert() {
        registerInsert(5, i1, txn1);
        registerInsert(5, i2, txn2);
        merge();
        assertTrue(getTxnLocal(i1, txn1).lookup(5));
    }

    @Test
    public void mergeConcurrentRemove() {
        registerRemove(5, new HashSet<TripleTimestamp>(), i1, txn1);
        registerRemove(5, new HashSet<TripleTimestamp>(), i2, txn2);
        merge();
        assertTrue(!getTxnLocal(i1, txn1).lookup(5));
    }

    @Test
    public void mergeConcurrentAddRem() {
        TripleTimestamp ts = registerInsert(5, i1, txn1);

        registerRemove(5, new HashSet<TripleTimestamp>(), i2, txn2);
        merge();
        assertTrue(getTxnLocal(i1, txn1).lookup(5));

        Set<TripleTimestamp> rm = new HashSet<TripleTimestamp>();
        rm.add(ts);
        registerRemove(5, rm, i1, txn1);
        assertTrue(!getTxnLocal(i1, txn1).lookup(5));
    }

    @Test
    public void mergeConcurrentCausal() {
        TripleTimestamp ts = registerInsert(5, i1, txn1);
        Set<TripleTimestamp> rm = new HashSet<TripleTimestamp>();
        rm.add(ts);
        registerRemove(5, rm, i1, txn1);

        registerInsert(5, i2, txn2);
        merge();
        assertTrue(getTxnLocal(i1, txn1).lookup(5));
    }

    // TODO Add tests for pruning!

}
