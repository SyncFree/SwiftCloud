/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
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
import swift.crdt.operations.DirectoryCreateUpdate;
import swift.crdt.operations.DirectoryRemoveUpdate;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

/**
 * @author annettebieniusa
 * 
 */
// TODO: All CRDTs w/mappings need a test for multi-mappings and pruning
public class DirectoryMergeTest {
    SwiftTester swift1, swift2;
    DirectoryVersioned i1, i2;
    String dirTable = "DIR";

    private void merge() {
        swift1.merge(i1, i2, swift2);
    }

    private DirectoryTxnLocal getTxnLocal(DirectoryVersioned i, TxnTester txn) {
        return (DirectoryTxnLocal) TesterUtils.getTxnLocal(i, txn);
    }

    private TripleTimestamp registerSingleInsertTxn(String entry, DirectoryVersioned i, SwiftTester swift) {
        final TxnTester txn = swift.beginTxn();
        try {
            return registerInsert(entry, i, txn);
        } finally {
            txn.commit(false);
        }
    }

    private TripleTimestamp registerInsert(String child, DirectoryVersioned i, TxnTester txn) {
        TripleTimestamp ts = txn.nextTimestamp();
        CRDTIdentifier childId = DirectoryTxnLocal.getCRDTIdentifier(dirTable, DirectoryTxnLocal.getFullPath(i.id),
                child, DirectoryVersioned.class);
        txn.registerOperation(i, new DirectoryCreateUpdate(childId, ts));
        return ts;
    }

    private void registerSingleRemoveTxn(String entry, Set<TripleTimestamp> rems, DirectoryVersioned i,
            SwiftTester swift) {
        final TxnTester txn = swift.beginTxn();
        registerRemove(entry, rems, i, txn);
        txn.commit();
    }

    private void registerRemove(String child, Set<TripleTimestamp> rems, DirectoryVersioned i, TxnTester txn) {
        CRDTIdentifier childId = DirectoryTxnLocal.getCRDTIdentifier(dirTable, DirectoryTxnLocal.getFullPath(i.id),
                child, DirectoryVersioned.class);
        txn.registerOperation(i, new DirectoryRemoveUpdate(childId, rems, txn.nextTimestamp()));
    }

    @Before
    public void setUp() throws WrongTypeException, NoSuchObjectException, VersionNotFoundException {
        i1 = new DirectoryVersioned();
        i1.init(new CRDTIdentifier(dirTable, DirectoryTxnLocal.getDirEntry("/root", DirectoryVersioned.class)),
                ClockFactory.newClock(), ClockFactory.newClock(), true);

        i2 = new DirectoryVersioned();
        i2.init(new CRDTIdentifier(dirTable, DirectoryTxnLocal.getDirEntry("/root", DirectoryVersioned.class)),
                ClockFactory.newClock(), ClockFactory.newClock(), true);
        swift1 = new SwiftTester("client1");
        swift2 = new SwiftTester("client2");
    }

    // Merge with empty set
    @Test
    public void mergeEmpty1() {
        registerSingleInsertTxn("x", i1, swift1);
        merge();
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).contains("x", DirectoryVersioned.class));
    }

    // Merge with empty set
    @Test
    public void mergeEmpty2() {
        registerSingleInsertTxn("x", i2, swift2);
        merge();
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).contains("x", DirectoryVersioned.class));
    }

    @Test
    public void mergeNonEmpty() {
        registerSingleInsertTxn("x", i1, swift1);
        registerSingleInsertTxn("y", i2, swift2);
        merge();
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).contains("x", DirectoryVersioned.class));
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).contains("y", DirectoryVersioned.class));
    }

    @Test
    public void mergeConcurrentInsert() {
        registerSingleInsertTxn("x", i1, swift1);
        registerSingleInsertTxn("x", i2, swift2);
        merge();
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).contains("x", DirectoryVersioned.class));
    }

    @Test
    public void mergeConcurrentRemove() {
        registerSingleRemoveTxn("x", new HashSet<TripleTimestamp>(), i1, swift1);
        registerSingleRemoveTxn("x", new HashSet<TripleTimestamp>(), i2, swift2);
        merge();
        assertTrue(!getTxnLocal(i1, swift1.beginTxn()).contains("x", DirectoryVersioned.class));
    }

    @Test
    public void mergeConcurrentAddRem() {
        TripleTimestamp ts = registerSingleInsertTxn("x", i1, swift1);

        registerSingleRemoveTxn("x", new HashSet<TripleTimestamp>(), i2, swift2);
        merge();
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).contains("x", DirectoryVersioned.class));

        Set<TripleTimestamp> rm = new HashSet<TripleTimestamp>();
        rm.add(ts);
        registerSingleRemoveTxn("x", rm, i1, swift1);
        assertTrue(!getTxnLocal(i1, swift1.beginTxn()).contains("x", DirectoryVersioned.class));
    }

    @Test
    public void mergeConcurrentCausal() {
        TripleTimestamp ts = registerSingleInsertTxn("x", i1, swift1);
        Set<TripleTimestamp> rm = new HashSet<TripleTimestamp>();
        rm.add(ts);
        registerSingleRemoveTxn("x", rm, i1, swift1);

        registerSingleInsertTxn("x", i2, swift2);
        merge(); //
        TesterUtils.printInformtion(i1, swift1.beginTxn());
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).contains("x", DirectoryVersioned.class));
    }

    @Test
    public void prune1() {
        registerSingleInsertTxn("z", i1, swift1);
        CausalityClock c = swift1.beginTxn().getClock();

        swift1.prune(i1, c);
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).contains("z", DirectoryVersioned.class));
    }

    @Test
    public void prune2() {
        registerSingleInsertTxn("x", i1, swift1);
        CausalityClock c = swift1.beginTxn().getClock();
        registerSingleInsertTxn("y", i1, swift1);

        swift1.prune(i1, c);
        // TesterUtils.printInformtion(i1,swift1.beginTxn());
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).contains("x", DirectoryVersioned.class));
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).contains("y", DirectoryVersioned.class));
    }

    @Test(expected = IllegalStateException.class)
    public void prune3() {
        for (int i = 0; i < 5; i++) {
            registerSingleInsertTxn(Integer.toString(i), i1, swift1);
        }
        CausalityClock c1 = swift1.beginTxn().getClock();

        Map<String, Set<TripleTimestamp>> rems = new HashMap<String, Set<TripleTimestamp>>();
        for (int i = 0; i < 5; i++) {
            TripleTimestamp ts = registerSingleInsertTxn(Integer.toString(i), i1, swift1);
            Set<TripleTimestamp> s = new HashSet<TripleTimestamp>();
            s.add(ts);
            rems.put(Integer.toString(i), s);
        }
        CausalityClock c2 = swift1.beginTxn().getClock();

        for (int i = 0; i < 5; i++) {
            registerSingleRemoveTxn(Integer.toString(i), rems.get(Integer.toString(i)), i1, swift1);
        }

        swift1.prune(i1, c2);
        // TesterUtils.printInformtion(i1,swift1.beginTxn());
        for (int i = 0; i < 5; i++) {
            assertTrue(getTxnLocal(i1, swift1.beginTxn()).contains(Integer.toString(i), DirectoryVersioned.class));
        } // should throw an exception
        i1.getTxnLocalCopy(c1, swift2.beginTxn());
    }

    @Test
    public void mergePruned1() {
        for (int i = 0; i < 5; i++) {
            registerSingleInsertTxn(Integer.toString(i), i1, swift1);
        }

        for (int i = 5; i < 10; i++) {
            registerSingleInsertTxn(Integer.toString(i), i2, swift2);
        }
        CausalityClock c2 = swift2.beginTxn().getClock();
        swift2.prune(i2, c2);
        swift1.merge(i1, i2, swift2);

        // TesterUtils.printInformtion(i1, swift1.beginTxn());
        for (int i = 0; i < 10; i++) {
            assertTrue(getTxnLocal(i1, swift1.beginTxn()).contains(Integer.toString(i), DirectoryVersioned.class));
        }
    }

    @Test
    public void mergePruned2() {
        for (int i = 0; i < 5; i++) {
            registerSingleInsertTxn(Integer.toString(i), i1, swift1);
        }
        CausalityClock c1 = swift1.beginTxn().getClock();
        swift1.prune(i1, c1);

        for (int i = 5; i < 10; i++) {
            registerSingleInsertTxn(Integer.toString(i), i2, swift2);
        }
        swift1.merge(i1, i2, swift2);

        // TesterUtils.printInformtion(i1, swift1.beginTxn());
        for (int i = 0; i < 10; i++) {
            assertTrue(getTxnLocal(i1, swift1.beginTxn()).contains(Integer.toString(i), DirectoryVersioned.class));
        }
    }

    @Test
    public void mergePruned3() {

        Map<String, Set<TripleTimestamp>> rems = new HashMap<String, Set<TripleTimestamp>>();
        for (int i = 0; i < 5; i++) {
            TripleTimestamp ts = registerSingleInsertTxn(Integer.toString(i), i1, swift1);
            Set<TripleTimestamp> s = new HashSet<TripleTimestamp>();
            s.add(ts);
            rems.put(Integer.toString(i), s);
        }
        CausalityClock c1 = swift1.beginTxn().getClock();
        for (int i = 0; i < 5; i++) {
            registerSingleRemoveTxn(Integer.toString(i), rems.get(Integer.toString(i)), i1, swift1);
        }
        swift1.prune(i1, c1);

        Map<String, Set<TripleTimestamp>> rems2 = new HashMap<String, Set<TripleTimestamp>>();
        for (int i = 0; i < 5; i++) {
            TripleTimestamp ts = registerSingleInsertTxn(Integer.toString(i), i2, swift2);
            Set<TripleTimestamp> s = new HashSet<TripleTimestamp>();
            s.add(ts);
            rems2.put(Integer.toString(i), s);
        }
        CausalityClock c2 = swift2.beginTxn().getClock();
        for (int i = 0; i < 5; i++) {
            registerSingleRemoveTxn(Integer.toString(i), rems2.get(Integer.toString(i)), i2, swift2);
        }

        swift2.prune(i2, c2);
        swift1.merge(i1, i2, swift2);

        // TesterUtils.printInformtion(i1, swift1.beginTxn());
        for (int i = 0; i < 6; i++) {
            assertTrue(!getTxnLocal(i1, swift1.beginTxn()).contains(Integer.toString(i), DirectoryVersioned.class));
        }

        swift1.prune(i1, swift1.beginTxn().getClock());
        // TesterUtils.printInformtion(i1, swift1.beginTxn());
    }

    @Test
    public void testTimestampsInUse() {
        final CausalityClock updatesSince = i1.getClock().clone();
        assertTrue(i1.getUpdatesTimestampMappingsSince(updatesSince).isEmpty());

        final TripleTimestamp ts = registerSingleInsertTxn("x", i1, swift1);
        registerSingleRemoveTxn("x", Collections.singleton(ts), i1, swift1);
        assertEquals(2, i1.getUpdatesTimestampMappingsSince(updatesSince).size());
    }
}
