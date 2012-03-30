package swift.test.crdt;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import swift.clocks.CausalityClock;
import swift.clocks.ClockFactory;
import swift.crdt.IntegerTxnLocal;
import swift.crdt.IntegerVersioned;
import swift.crdt.operations.IntegerUpdate;
import swift.exceptions.ConsistentSnapshotVersionNotFoundException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.WrongTypeException;

public class IntegerMergeTest {
    IntegerVersioned i1, i2;
    SwiftTester swift1, swift2;

    private IntegerTxnLocal getTxnLocal(IntegerVersioned i, TxnTester txn) {
        return (IntegerTxnLocal) TesterUtils.getTxnLocal(i, txn);
    }

    private void merge() {
        swift1.merge(i1, i2, swift2);
    }

    private void registerUpdate(int value, IntegerVersioned i, TxnTester txn) {
        txn.registerOperation(i, new IntegerUpdate(txn.nextTimestamp(), value));
    }

    @Before
    public void setUp() throws WrongTypeException, NoSuchObjectException, ConsistentSnapshotVersionNotFoundException {
        i1 = new IntegerVersioned();
        i1.setClock(ClockFactory.newClock());
        i1.setPruneClock(ClockFactory.newClock());

        i2 = new IntegerVersioned();
        i2.setClock(ClockFactory.newClock());
        i2.setPruneClock(ClockFactory.newClock());
        swift1 = new SwiftTester("client1");
        swift2 = new SwiftTester("client2");
    }

    // Merge with empty set
    @Test
    public void mergeEmpty1() {
        registerUpdate(5, i1, swift1.beginTxn());
        i1.merge(i2);

        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == 5);
    }

    // Merge with empty set
    @Test
    public void mergeEmpty2() {
        registerUpdate(5, i2, swift2.beginTxn());
        i1.merge(i2);
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == 5);
    }

    @Test
    public void mergeNonEmpty() {
        registerUpdate(5, i1, swift1.beginTxn());
        registerUpdate(6, i2, swift2.beginTxn());
        i1.merge(i2);
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == 11);
    }

    @Test
    public void mergeConcurrentAddRem() {
        registerUpdate(5, i1, swift1.beginTxn());
        registerUpdate(-5, i2, swift2.beginTxn());
        merge();
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == 0);

        registerUpdate(-5, i1, swift1.beginTxn());
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == -5);
    }

    @Test
    public void mergeConcurrentCausal() {
        registerUpdate(5, i1, swift1.beginTxn());
        registerUpdate(-5, i1, swift1.beginTxn());
        registerUpdate(5, i2, swift2.beginTxn());
        merge();
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == 5);
    }

    @Test
    public void mergeConcurrentCausal2() {
        registerUpdate(5, i1, swift1.beginTxn());
        registerUpdate(-5, i1, swift1.beginTxn());
        registerUpdate(5, i2, swift2.beginTxn());
        registerUpdate(-5, i2, swift2.beginTxn());
        merge();
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == 0);

        registerUpdate(-5, i2, swift2.beginTxn());
        merge();
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == -5);
    }

    @Test
    public void prune1() {
        registerUpdate(1, i1, swift1.beginTxn());
        CausalityClock c = swift1.beginTxn().getClock();

        swift1.prune(i1, c);
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == 1);

    }

    @Test
    public void prune2() {
        registerUpdate(1, i1, swift1.beginTxn());
        CausalityClock c = swift1.beginTxn().getClock();
        registerUpdate(2, i1, swift1.beginTxn());

        swift1.prune(i1, c);
        // TesterUtils.printInformtion(i1, swift1.beginTxn());
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == 3);
    }

    @Test(expected = IllegalArgumentException.class)
    public void prune3() {
        for (int i = 0; i < 5; i++) {
            registerUpdate(1, i1, swift1.beginTxn());
        }
        CausalityClock c1 = swift1.beginTxn().getClock();

        for (int i = 0; i < 5; i++) {
            registerUpdate(1, i1, swift1.beginTxn());
        }
        CausalityClock c2 = swift1.beginTxn().getClock();

        for (int i = 0; i < 5; i++) {
            registerUpdate(1, i1, swift1.beginTxn());
        }

        swift1.prune(i1, c2);
        TesterUtils.printInformtion(i1, swift1.beginTxn());
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == 15);

        i1.getTxnLocalCopy(c1, swift2.beginTxn());
    }

    @Test
    public void mergePruned1() {
        for (int i = 0; i < 5; i++) {
            registerUpdate(1, i1, swift1.beginTxn());
        }

        for (int i = 0; i < 5; i++) {
            registerUpdate(1, i2, swift2.beginTxn());
        }
        CausalityClock c2 = swift2.beginTxn().getClock();
        swift2.prune(i2, c2);
        swift1.merge(i1, i2, swift2);

        // TesterUtils.printInformtion(i1, swift1.beginTxn());
        int result = getTxnLocal(i1, swift1.beginTxn()).getValue();
        assertTrue(result == 10);
    }

    @Test
    public void mergePruned2() {
        for (int i = 0; i < 5; i++) {
            registerUpdate(1, i1, swift1.beginTxn());
        }
        CausalityClock c1 = swift1.beginTxn().getClock();
        swift1.prune(i1, c1);

        for (int i = 0; i < 5; i++) {
            registerUpdate(1, i2, swift2.beginTxn());
        }
        swift1.merge(i1, i2, swift2);

        // TesterUtils.printInformtion(i1, swift1.beginTxn());
        int result = getTxnLocal(i1, swift1.beginTxn()).getValue();
        assertTrue(result == 10);
    }

    @Test
    public void mergePruned3() {
        for (int i = 0; i < 5; i++) {
            registerUpdate(1, i1, swift1.beginTxn());
        }
        CausalityClock c1 = swift1.beginTxn().getClock();
        for (int i = 0; i < 5; i++) {
            registerUpdate(1, i1, swift1.beginTxn());
        }
        swift1.prune(i1, c1);

        for (int i = 0; i < 5; i++) {
            registerUpdate(1, i2, swift2.beginTxn());
        }

        CausalityClock c2 = swift2.beginTxn().getClock();
        for (int i = 0; i < 5; i++) {
            registerUpdate(1, i2, swift2.beginTxn());
        }
        swift2.prune(i2, c2);
        swift1.merge(i1, i2, swift2);

        // TesterUtils.printInformtion(i1, swift1.beginTxn());
        int result = getTxnLocal(i1, swift1.beginTxn()).getValue();
        assertTrue(result == 20);
    }

}
