package swift.crdt;

import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import swift.application.social.Message;
import swift.clocks.ClockFactory;
import swift.clocks.TripleTimestamp;
import swift.crdt.operations.SetInsert;
import swift.crdt.operations.SetRemove;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

public class SetMergeTestData {
    SwiftTester swift1, swift2;
    SetMsg i1, i2;

    private void merge() {
        swift1.merge(i1, i2, swift2);
    }

    private SetTxnLocalMsg getTxnLocal(SetMsg i, TxnTester txn) {
        return (SetTxnLocalMsg) TesterUtils.getTxnLocal(i, txn);
    }

    private TripleTimestamp registerSingleInsertTxn(Message value, SetMsg i, SwiftTester swift) {
        final TxnTester txn = swift.beginTxn();
        try {
            return registerInsert(value, i, txn);
        } finally {
            txn.commit();
        }
    }

    private TripleTimestamp registerInsert(Message msg, SetMsg i, TxnTester txn) {
        TripleTimestamp ts = txn.nextTimestamp();
        txn.registerOperation(i, new SetInsert<Message, SetMsg>(ts, msg));
        return ts;
    }

    private void registerSingleRemoveTxn(Message value, Set<TripleTimestamp> rems, SetMsg i, SwiftTester swift) {
        final TxnTester txn = swift.beginTxn();
        registerRemove(value, rems, i, txn);
        txn.commit();
    }

    private void registerRemove(Message value, Set<TripleTimestamp> rems, SetMsg i, TxnTester txn) {
        txn.registerOperation(i, new SetRemove<Message, SetMsg>(txn.nextTimestamp(), value, rems));
    }

    @Before
    public void setUp() throws WrongTypeException, NoSuchObjectException, VersionNotFoundException {
        i1 = new SetMsg();
        i1.init(null, ClockFactory.newClock(), ClockFactory.newClock(), true);

        i2 = new SetMsg();
        i2.init(null, ClockFactory.newClock(), ClockFactory.newClock(), true);
        swift1 = new SwiftTester("client1");
        swift2 = new SwiftTester("client2");
    }

    @Test
    public void idemPotentMerge() {
        registerSingleInsertTxn(new Message("Blub", "Blah", "Autsch", System.currentTimeMillis()), i1, swift1);
        SetMsg iclone = i1.copy();
        swift1.merge(i1, iclone, swift2);
        TesterUtils.printInformtion(iclone, swift1.beginTxn());
    }
}
