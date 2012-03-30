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
    SwiftTester swift1, swift2;
    SetIntegers i1, i2;

    private void merge() {
        swift1.merge(i1, i2, swift2);
    }

    private SetTxnLocalInteger getTxnLocal(SetIntegers i, TxnTester txn) {
        return (SetTxnLocalInteger) TesterUtils.getTxnLocal(i, txn);
    }

    private TripleTimestamp registerSingleInsertTxn(int value, SetIntegers i, SwiftTester swift) {
        final TxnTester txn = swift.beginTxn();
        try {
            return registerInsert(value, i, txn);
        } finally {
            txn.commit(true);
        }
    }

    private TripleTimestamp registerInsert(int value, SetIntegers i, TxnTester txn) {
        TripleTimestamp ts = txn.nextTimestamp();
        txn.registerOperation(i, new SetInsert<Integer, SetIntegers>(ts, value));
        return ts;
    }

    private void registerSingleRemoveTxn(int value, Set<TripleTimestamp> rems, SetIntegers i, SwiftTester swift) {
        final TxnTester txn = swift.beginTxn();
        registerRemove(value, rems, i, txn);
        txn.commit(true);
    }

    private void registerRemove(int value, Set<TripleTimestamp> rems, SetIntegers i, TxnTester txn) {
        txn.registerOperation(i, new SetRemove<Integer, SetIntegers>(txn.nextTimestamp(), value, rems));
    }

    @Before
    public void setUp() throws WrongTypeException, NoSuchObjectException, ConsistentSnapshotVersionNotFoundException {
        i1 = new SetIntegers();
        i1.init(null, ClockFactory.newClock(), ClockFactory.newClock(), true);

        i2 = new SetIntegers();
        i2.init(null, ClockFactory.newClock(), ClockFactory.newClock(), true);
        swift1 = new SwiftTester("client1");
        swift2 = new SwiftTester("client2");
    }

    // Merge with empty set
    @Test
    public void mergeEmpty1() {
        registerSingleInsertTxn(5, i1, swift1);
        merge();
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).lookup(5));
    }

    // Merge with empty set
    @Test
    public void mergeEmpty2() {
        registerSingleInsertTxn(5, i2, swift2);
        merge();
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).lookup(5));
    }

    @Test
    public void mergeNonEmpty() {
        registerSingleInsertTxn(5, i1, swift1);
        registerSingleInsertTxn(6, i2, swift2);
        merge();
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).lookup(5));
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).lookup(6));
    }

    @Test
    public void mergeConcurrentInsert() {
        registerSingleInsertTxn(5, i1, swift1);
        registerSingleInsertTxn(5, i2, swift2);
        merge();
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).lookup(5));
    }

    @Test
    public void mergeConcurrentRemove() {
        registerSingleRemoveTxn(5, new HashSet<TripleTimestamp>(), i1, swift1);
        registerSingleRemoveTxn(5, new HashSet<TripleTimestamp>(), i2, swift2);
        merge();
        assertTrue(!getTxnLocal(i1, swift1.beginTxn()).lookup(5));
    }

    @Test
    public void mergeConcurrentAddRem() {
        TripleTimestamp ts = registerSingleInsertTxn(5, i1, swift1);

        registerSingleRemoveTxn(5, new HashSet<TripleTimestamp>(), i2, swift2);
        merge();
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).lookup(5));

        Set<TripleTimestamp> rm = new HashSet<TripleTimestamp>();
        rm.add(ts);
        registerSingleRemoveTxn(5, rm, i1, swift1);
        assertTrue(!getTxnLocal(i1, swift1.beginTxn()).lookup(5));
    }

    @Test
    public void mergeConcurrentCausal() {
        TripleTimestamp ts = registerSingleInsertTxn(5, i1, swift1);
        Set<TripleTimestamp> rm = new HashSet<TripleTimestamp>();
        rm.add(ts);
        registerSingleRemoveTxn(5, rm, i1, swift1);

        registerSingleInsertTxn(5, i2, swift2);
        merge();
        TesterUtils.printInformtion(i1, swift1.beginTxn());
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).lookup(5));
    }

    // TODO Add tests for pruning!

}
