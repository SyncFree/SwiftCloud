package swift.crdt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import swift.clocks.CausalityClock;
import swift.clocks.ClockFactory;
import swift.clocks.TripleTimestamp;
import swift.crdt.operations.SetInsert;
import swift.crdt.operations.SetRemove;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
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
            txn.commit();
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
        txn.commit();
    }

    private void registerRemove(int value, Set<TripleTimestamp> rems, SetIntegers i, TxnTester txn) {
        txn.registerOperation(i, new SetRemove<Integer, SetIntegers>(txn.nextTimestamp(), value, rems));
    }

    @Before
    public void setUp() throws WrongTypeException, NoSuchObjectException, VersionNotFoundException {
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
        // TesterUtils.printInformtion(i1, swift1.beginTxn());
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).lookup(5));
    }

    @Test
    public void prune1() {
        registerSingleInsertTxn(1, i1, swift1);
        CausalityClock c = swift1.beginTxn().getClock();

        swift1.prune(i1, c);
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).lookup(1));
    }

    @Test
    public void prune2() {
        registerSingleInsertTxn(1, i1, swift1);
        CausalityClock c = swift1.beginTxn().getClock();
        registerSingleInsertTxn(2, i1, swift1);

        swift1.prune(i1, c);
        // TesterUtils.printInformtion(i1, swift1.beginTxn());
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).lookup(1));
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).lookup(2));
    }

    @Test(expected = IllegalStateException.class)
    public void prune3() {
        for (int i = 0; i < 5; i++) {
            registerSingleInsertTxn(i, i1, swift1);
        }
        CausalityClock c1 = swift1.beginTxn().getClock();

        Map<Integer, Set<TripleTimestamp>> rems = new HashMap<Integer, Set<TripleTimestamp>>();
        for (int i = 0; i < 5; i++) {
            TripleTimestamp ts = registerSingleInsertTxn(i, i1, swift1);
            Set<TripleTimestamp> s = new HashSet<TripleTimestamp>();
            s.add(ts);
            rems.put(i, s);
        }
        CausalityClock c2 = swift1.beginTxn().getClock();

        for (int i = 0; i < 5; i++) {
            registerSingleRemoveTxn(i, rems.get(i), i1, swift1);
        }

        swift1.prune(i1, c2);
        // TesterUtils.printInformtion(i1, swift1.beginTxn());
        for (int i = 0; i < 5; i++) {
            assertTrue(getTxnLocal(i1, swift1.beginTxn()).lookup(i));
        }
        // should throw an exception
        i1.getTxnLocalCopy(c1, swift2.beginTxn());
    }

    @Test
    public void mergePruned1() {
        for (int i = 0; i < 5; i++) {
            registerSingleInsertTxn(i, i1, swift1);
        }

        for (int i = 5; i < 10; i++) {
            registerSingleInsertTxn(i, i2, swift2);
        }
        CausalityClock c2 = swift2.beginTxn().getClock();
        swift2.prune(i2, c2);
        swift1.merge(i1, i2, swift2);

        // TesterUtils.printInformtion(i1, swift1.beginTxn());
        for (int i = 0; i < 10; i++) {
            assertTrue(getTxnLocal(i1, swift1.beginTxn()).lookup(i));
        }
    }

    @Test
    public void mergePruned2() {
        for (int i = 0; i < 5; i++) {
            registerSingleInsertTxn(i, i1, swift1);
        }
        CausalityClock c1 = swift1.beginTxn().getClock();
        swift1.prune(i1, c1);

        for (int i = 5; i < 10; i++) {
            registerSingleInsertTxn(i, i2, swift2);
        }
        swift1.merge(i1, i2, swift2);

        // TesterUtils.printInformtion(i1, swift1.beginTxn());
        for (int i = 0; i < 10; i++) {
            assertTrue(getTxnLocal(i1, swift1.beginTxn()).lookup(i));
        }
    }

    @Test
    public void mergePruned3() {

        Map<Integer, Set<TripleTimestamp>> rems = new HashMap<Integer, Set<TripleTimestamp>>();
        for (int i = 0; i < 5; i++) {
            TripleTimestamp ts = registerSingleInsertTxn(i, i1, swift1);
            Set<TripleTimestamp> s = new HashSet<TripleTimestamp>();
            s.add(ts);
            rems.put(i, s);
        }
        CausalityClock c1 = swift1.beginTxn().getClock();
        for (int i = 0; i < 5; i++) {
            registerSingleRemoveTxn(i, rems.get(i), i1, swift1);
        }
        swift1.prune(i1, c1);

        Map<Integer, Set<TripleTimestamp>> rems2 = new HashMap<Integer, Set<TripleTimestamp>>();
        for (int i = 0; i < 5; i++) {
            TripleTimestamp ts = registerSingleInsertTxn(i, i1, swift1);
            Set<TripleTimestamp> s = new HashSet<TripleTimestamp>();
            s.add(ts);
            rems2.put(i, s);
        }
        CausalityClock c2 = swift2.beginTxn().getClock();
        for (int i = 0; i < 5; i++) {
            registerSingleRemoveTxn(i, rems2.get(i), i2, swift2);
        }

        swift2.prune(i2, c2);
        swift1.merge(i1, i2, swift2);

        // TesterUtils.printInformtion(i1, swift1.beginTxn());
        for (int i = 0; i < 6; i++) {
            assertTrue(!getTxnLocal(i1, swift1.beginTxn()).lookup(i));
        }

        swift1.prune(i1, swift1.beginTxn().getClock());
        // TesterUtils.printInformtion(i1, swift1.beginTxn());
    }

    @Test
    public void testGetUpdateTimestampsSince() {
        final CausalityClock updatesSince = i1.getClock().clone();
        assertTrue(i1.getUpdateTimestampsSince(updatesSince).isEmpty());

        final TripleTimestamp ts = registerSingleInsertTxn(1, i1, swift1);
        registerSingleRemoveTxn(1, Collections.singleton(ts), i1, swift1);
        assertEquals(2, i1.getUpdateTimestampsSince(updatesSince).size());
    }

    @Test
    public void idemPotentMerge() {
        registerSingleInsertTxn(15, i1, swift1);
        SetIntegers iclone = i1.copy();
        swift1.merge(i1, iclone, swift2);
        TesterUtils.printInformtion(iclone, swift1.beginTxn());
    }
}
