package swift.crdt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import swift.clocks.CausalityClock;
import swift.clocks.ClockFactory;
import swift.crdt.interfaces.Copyable;
import swift.crdt.operations.RegisterUpdate;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

// TODO: tests for concurrent RegisterUpdates or sequential RegisterUpdates with some interleavings.
public class RegisterMergeTest {
    RegisterVersioned<IntegerWrap> i1, i2;
    SwiftTester swift1, swift2;

    private <V extends Copyable> RegisterTxnLocal<V> getTxnLocal(RegisterVersioned<V> i, TxnTester txn) {
        return (RegisterTxnLocal<V>) TesterUtils.getTxnLocal(i, txn);
    }

    private void merge() {
        swift1.merge(i1, i2, swift2);
    }

    private void registerSingleUpdateTxn(int value, RegisterVersioned<IntegerWrap> i, SwiftTester swift) {
        final TxnTester txn = swift.beginTxn();
        registerUpdate(value, i, txn);
        txn.commit();
    }

    private void registerUpdate(int value, RegisterVersioned<IntegerWrap> i, TxnTester txn) {
        txn.registerOperation(i,
                new RegisterUpdate<IntegerWrap>(txn.nextTimestamp(), new IntegerWrap(value), txn.getClock()));
    }

    @Before
    public void setUp() throws WrongTypeException, NoSuchObjectException, VersionNotFoundException {
        i1 = new RegisterVersioned<IntegerWrap>();
        i1.init(null, ClockFactory.newClock(), ClockFactory.newClock(), true);

        i2 = new RegisterVersioned<IntegerWrap>();
        i2.init(null, ClockFactory.newClock(), ClockFactory.newClock(), true);
        swift1 = new SwiftTester("client1");
        swift2 = new SwiftTester("client2");
    }

    // Merge with empty set
    @Test
    public void mergeEmpty1() {
        registerSingleUpdateTxn(5, i1, swift1);
        merge();

        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue().i == 5);
    }

    // Merge with empty set
    @Test
    public void mergeEmpty2() {
        registerSingleUpdateTxn(5, i2, swift2);
        i1.merge(i2);
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue().i == 5);
    }

    @Test
    public void mergeNonEmpty() {
        registerSingleUpdateTxn(5, i1, swift1);
        registerSingleUpdateTxn(6, i2, swift2);
        i1.merge(i2);
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue().i == 6
                || getTxnLocal(i1, swift1.beginTxn()).getValue().i == 5);
    }

    @Test
    public void mergeMultiple() {
        registerSingleUpdateTxn(5, i2, swift2);
        registerSingleUpdateTxn(6, i2, swift2);
        i1.merge(i2);
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue().i == 6);
    }

    @Test
    public void mergeConcurrentAddRem() {
        registerSingleUpdateTxn(5, i1, swift1);
        registerSingleUpdateTxn(-5, i2, swift2);
        merge();
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue().i == -5
                || getTxnLocal(i1, swift1.beginTxn()).getValue().i == 5);

        registerSingleUpdateTxn(2, i1, swift1);
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue().i == 2);
    }

    @Test
    public void mergeConcurrentCausal() {
        registerSingleUpdateTxn(1, i1, swift1);
        registerSingleUpdateTxn(-1, i1, swift1);
        registerSingleUpdateTxn(2, i2, swift2);
        merge();
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue().i == 2
                || getTxnLocal(i1, swift1.beginTxn()).getValue().i == -1);
    }

    @Test
    public void mergeConcurrentCausal2() {
        registerSingleUpdateTxn(1, i1, swift1);
        registerSingleUpdateTxn(-1, i1, swift1);
        registerSingleUpdateTxn(2, i2, swift2);
        registerSingleUpdateTxn(-2, i2, swift2);
        merge();
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue().i == -1
                || getTxnLocal(i1, swift1.beginTxn()).getValue().i == -2);

        registerSingleUpdateTxn(-5, i2, swift2);
        merge();
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue().i == -5);
    }

    @Test
    public void prune1() {
        registerSingleUpdateTxn(1, i1, swift1);
        CausalityClock c = swift1.beginTxn().getClock();

        swift1.prune(i1, c);
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue().i == 1);

    }

    @Test
    public void prune2() {
        registerSingleUpdateTxn(1, i1, swift1);
        CausalityClock c = swift1.beginTxn().getClock();
        registerSingleUpdateTxn(2, i1, swift1);

        swift1.prune(i1, c);
        // TesterUtils.printInformtion(i1, swift1.beginTxn());
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue().i == 2);
    }

    @Test(expected = IllegalStateException.class)
    public void prune3() {
        for (int i = 0; i < 5; i++) {
            registerSingleUpdateTxn(i, i1, swift1);
        }
        CausalityClock c1 = swift1.beginTxn().getClock();

        for (int i = 0; i < 5; i++) {
            registerSingleUpdateTxn(i, i1, swift1);
        }
        CausalityClock c2 = swift1.beginTxn().getClock();

        for (int i = 10; i < 20; i++) {
            registerSingleUpdateTxn(i, i1, swift1);
        }

        swift1.prune(i1, c2);
        // TesterUtils.printInformtion(i1, swift1.beginTxn());
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue().i == 19);

        i1.getTxnLocalCopy(c1, swift2.beginTxn());
    }

    @Test
    public void mergePruned1() {
        for (int i = 0; i < 5; i++) {
            registerSingleUpdateTxn(i + 10, i1, swift1);
        }

        for (int i = 0; i < 5; i++) {
            registerSingleUpdateTxn(i + 20, i2, swift2);
        }
        CausalityClock c2 = swift2.beginTxn().getClock();
        swift2.prune(i2, c2);
        swift1.merge(i1, i2, swift2);

        TesterUtils.printInformtion(i1, swift1.beginTxn());
        int result = getTxnLocal(i1, swift1.beginTxn()).getValue().i;
        assertTrue(result == 14 || result == 24);
    }

    @Test
    public void mergePruned2() {
        for (int i = 0; i < 5; i++) {
            registerSingleUpdateTxn(i + 10, i1, swift1);
        }
        CausalityClock c1 = swift1.beginTxn().getClock();
        swift1.prune(i1, c1);

        for (int i = 0; i < 5; i++) {
            registerSingleUpdateTxn(i + 20, i2, swift2);
        }
        swift1.merge(i1, i2, swift2);

        TesterUtils.printInformtion(i1, swift1.beginTxn());
        int result = getTxnLocal(i1, swift1.beginTxn()).getValue().i;
        assertTrue(result == 14 || result == 24);
    }

    @Test
    public void mergePruned3() {
        for (int i = 0; i < 2; i++) {
            registerSingleUpdateTxn(i + 10, i1, swift1);
        }
        CausalityClock c1 = swift1.beginTxn().getClock();
        for (int i = 2; i < 4; i++) {
            registerSingleUpdateTxn(i + 10, i1, swift1);
        }
        swift1.prune(i1, c1);

        for (int i = 0; i < 2; i++) {
            registerSingleUpdateTxn(i + 20, i2, swift2);
        }

        CausalityClock c2 = swift2.beginTxn().getClock();
        for (int i = 2; i < 4; i++) {
            registerSingleUpdateTxn(i + 20, i2, swift2);
        }
        swift2.prune(i2, c2);
        swift1.merge(i1, i2, swift2);

        TesterUtils.printInformtion(i1, swift1.beginTxn());
        int result = getTxnLocal(i1, swift1.beginTxn()).getValue().i;
        assertTrue(result == 13 || result == 23);
    }

    @Test
    public void testGetUpdateTimestampsSince() {
        final CausalityClock updatesSince = i1.getClock().clone();
        assertTrue(i1.getUpdateTimestampsSince(updatesSince).isEmpty());

        registerSingleUpdateTxn(1, i1, swift1);
        assertEquals(1, i1.getUpdateTimestampsSince(updatesSince).size());
    }
}
