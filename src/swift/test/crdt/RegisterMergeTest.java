package swift.test.crdt;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import swift.clocks.ClockFactory;
import swift.crdt.RegisterTxnLocal;
import swift.crdt.RegisterVersioned;
import swift.crdt.operations.RegisterUpdate;
import swift.exceptions.ConsistentSnapshotVersionNotFoundException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.WrongTypeException;

public class RegisterMergeTest {
    RegisterVersioned<Integer> i1, i2;
    SwiftTester swift1, swift2;

    private <V> RegisterTxnLocal<V> getTxnLocal(RegisterVersioned<V> i, TxnTester txn) {
        return (RegisterTxnLocal<V>) TesterUtils.getTxnLocal(i, txn);
    }

    private void merge() {
        swift1.merge(i1, i2, swift2);
    }

    private <V> void registerUpdate(V value, RegisterVersioned<V> i, TxnTester txn) {
        txn.registerOperation(i, new RegisterUpdate<V>(txn.nextTimestamp(), value));
    }

    @Before
    public void setUp() throws WrongTypeException, NoSuchObjectException, ConsistentSnapshotVersionNotFoundException {
        i1 = new RegisterVersioned<Integer>();
        i1.setClock(ClockFactory.newClock());
        i1.setPruneClock(ClockFactory.newClock());

        i2 = new RegisterVersioned<Integer>();
        i2.setClock(ClockFactory.newClock());
        i2.setPruneClock(ClockFactory.newClock());
        swift1 = new SwiftTester("client1");
        swift2 = new SwiftTester("client2");
    }

    // Merge with empty set
    @Test
    public void mergeEmpty1() {
        registerUpdate(5, i1, swift1.beginTxn());
        merge();

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
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == 6
                || getTxnLocal(i1, swift1.beginTxn()).getValue() == 5);
    }

    @Test
    public void mergeMultiple() {
        registerUpdate(5, i2, swift2.beginTxn());
        registerUpdate(6, i2, swift2.beginTxn());
        i1.merge(i2);
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == 6);
    }

    @Test
    public void mergeConcurrentAddRem() {
        registerUpdate(5, i1, swift1.beginTxn());
        registerUpdate(-5, i2, swift2.beginTxn());
        merge();
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == -5
                || getTxnLocal(i1, swift1.beginTxn()).getValue() == 5);

        registerUpdate(2, i1, swift1.beginTxn());
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == 2);
    }

    @Test
    public void mergeConcurrentCausal() {
        registerUpdate(1, i1, swift1.beginTxn());
        registerUpdate(-1, i1, swift1.beginTxn());
        registerUpdate(2, i2, swift2.beginTxn());
        merge();
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == 2
                || getTxnLocal(i1, swift1.beginTxn()).getValue() == -1);
    }

    @Test
    public void mergeConcurrentCausal2() {
        registerUpdate(1, i1, swift1.beginTxn());
        registerUpdate(-1, i1, swift1.beginTxn());
        registerUpdate(2, i2, swift2.beginTxn());
        registerUpdate(-2, i2, swift2.beginTxn());
        merge();
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == -1
                || getTxnLocal(i1, swift1.beginTxn()).getValue() == -2);

        registerUpdate(-5, i2, swift2.beginTxn());
        merge();
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == -5);
    }

    // TODO Tests for prune

}
