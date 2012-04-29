package swift.dc;

import swift.client.AbstractObjectUpdatesListener;
import swift.client.SwiftImpl;
import swift.crdt.CRDTIdentifier;
import swift.crdt.IntegerTxnLocal;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;
import sys.Sys;

public class TestClient {
    public static void main(String[] args) {
        try {
            Sys.init();

            SwiftImpl server0 = SwiftImpl.newInstance(0, "localhost", DCConstants.SURROGATE_PORT);

            SwiftImpl server = SwiftImpl.newInstance(0, "localhost", DCConstants.SURROGATE_PORT);
            TxnHandle handle = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT,
                    false);
            IntegerTxnLocal i1 = handle.get(new CRDTIdentifier("e", "1"), false, swift.crdt.IntegerVersioned.class,
                    new AbstractObjectUpdatesListener() {
                        @Override
                        public void onObjectUpdate(TxnHandle txn, CRDTIdentifier id, TxnLocalCRDT<?> previousValue) {
                            System.out.println("NOTIFY: object modified : " + id
                                    + "******************************************************");
                        }
                    });
            System.out.println("(e,1) = " + i1.getValue());
            IntegerTxnLocal i2 = handle.get(new CRDTIdentifier("e", "2"), false, swift.crdt.IntegerVersioned.class,
                    new DummyObjectUpdatesListener());
            System.out.println("(e,2) = " + i2.getValue());
            i1.add(1);
            System.out.println("(e,1).add(1)");
            System.out.println("(e,1) = " + i1.getValue());

            TxnHandle handle0 = server0.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT,
                    false);
            IntegerTxnLocal i0 = handle0.get(new CRDTIdentifier("e", "1"), false, swift.crdt.IntegerVersioned.class);
            i0.add(1);
            System.out.println( "Changing (e,1) in other transaction");
            handle0.commit();
            Thread.sleep(5000);

            handle.commit();
            System.out.println("commit");

            handle = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);
            i1 = handle.get(new CRDTIdentifier("e", "1"), false, swift.crdt.IntegerVersioned.class,
                    new DummyObjectUpdatesListener());
            System.out.println("(e,1) = " + i1.getValue());
            i2 = handle.get(new CRDTIdentifier("e", "2"), false, swift.crdt.IntegerVersioned.class,
                    new DummyObjectUpdatesListener());
            System.out.println("(e,2) = " + i2.getValue());
            i1.add(1);
            System.out.println("(e,1).add(1)");
            System.out.println("(e,1) = " + i1.getValue());
            handle.commit();
            System.out.println("commit");

            handle = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);
            i1 = handle.get(new CRDTIdentifier("t", "1"), true, swift.crdt.IntegerVersioned.class,
                    new DummyObjectUpdatesListener());
            System.out.println("(t,1) = " + i1.getValue());
            i2 = handle.get(new CRDTIdentifier("t", "2"), true, swift.crdt.IntegerVersioned.class,
                    new DummyObjectUpdatesListener());
            System.out.println("(t,2) = " + i2.getValue());
            i1.add(1);
            System.out.println("(t,1).add(1)");
            System.out.println("(t,1) = " + i1.getValue());
            handle.commit();
            System.out.println("commit");

            handle = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);
            i1 = handle.get(new CRDTIdentifier("t", "1"), false, swift.crdt.IntegerVersioned.class,
                    new DummyObjectUpdatesListener());
            System.out.println("(t,1) = " + i1.getValue());
            i2 = handle.get(new CRDTIdentifier("t", "2"), false, swift.crdt.IntegerVersioned.class,
                    new DummyObjectUpdatesListener());
            System.out.println("(t,2) = " + i2.getValue());
            i1.add(1);
            System.out.println("(t,1).add(1)");
            System.out.println("(t,1) = " + i1.getValue());
            handle.commit();
            System.out.println("commit");

            handle = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);
            i1 = handle.get(new CRDTIdentifier("t", "1"), false, swift.crdt.IntegerVersioned.class,
                    new DummyObjectUpdatesListener());
            System.out.println("(t,1) = " + i1.getValue());
            i2 = handle.get(new CRDTIdentifier("t", "2"), false, swift.crdt.IntegerVersioned.class,
                    new DummyObjectUpdatesListener());
            System.out.println("(t,2) = " + i2.getValue());
            i1.add(1);
            System.out.println("(t,1).add(1)");
            System.out.println("(t,1) = " + i1.getValue());
            handle.commit();
            System.out.println("commit");

            System.out.println("TetsClient ended with success");

            Thread.sleep(10000);
            server.stop(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(0);
    }

    static class DummyObjectUpdatesListener extends AbstractObjectUpdatesListener {
        @Override
        public void onObjectUpdate(TxnHandle txn, CRDTIdentifier id, TxnLocalCRDT<?> previousValue) {
            System.out.println("Yoohoo, the object " + id + " has changed!");
        }
    }
}
