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

import org.junit.Before;
import org.junit.Test;

import swift.clocks.CausalityClock;
import swift.clocks.ClockFactory;
import swift.crdt.operations.IntegerUpdate;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

// TODO: All CRDTs w/mappings need a test for multi-mappings and pruning
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

    private void registerSingleUpdateTxn(int value, IntegerVersioned i, SwiftTester swift) {
        final TxnTester txn = swift.beginTxn();
        registerUpdate(value, i, txn);
        txn.commit(true);
    }

    @Before
    public void setUp() throws WrongTypeException, NoSuchObjectException, VersionNotFoundException {
        i1 = new IntegerVersioned();
        i1.init(null, ClockFactory.newClock(), ClockFactory.newClock(), true);

        i2 = new IntegerVersioned();
        i2.init(null, ClockFactory.newClock(), ClockFactory.newClock(), true);
        swift1 = new SwiftTester("client1");
        swift2 = new SwiftTester("client2");
    }

    // Merge with empty set
    @Test
    public void mergeEmpty1() {
        registerSingleUpdateTxn(5, i1, swift1);
        i1.merge(i2);

        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == 5);
    }

    // Merge with empty set
    @Test
    public void mergeEmpty2() {
        registerSingleUpdateTxn(5, i2, swift2);
        i1.merge(i2);
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == 5);
    }

    @Test
    public void mergeNonEmpty() {
        registerSingleUpdateTxn(5, i1, swift1);
        registerSingleUpdateTxn(6, i2, swift2);
        i1.merge(i2);
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == 11);
    }

    @Test
    public void mergeConcurrentAddRem() {
        registerSingleUpdateTxn(5, i1, swift1);
        registerSingleUpdateTxn(-5, i2, swift2);
        merge();
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == 0);

        registerSingleUpdateTxn(-5, i1, swift1);
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == -5);
    }

    @Test
    public void mergeConcurrentCausal() {
        registerSingleUpdateTxn(5, i1, swift1);
        registerSingleUpdateTxn(-5, i1, swift1);
        registerSingleUpdateTxn(5, i2, swift2);
        merge();
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == 5);
    }

    @Test
    public void mergeConcurrentCausal2() {
        registerSingleUpdateTxn(5, i1, swift1);
        registerSingleUpdateTxn(-5, i1, swift1);
        registerSingleUpdateTxn(5, i2, swift2);
        registerSingleUpdateTxn(-5, i2, swift2);
        merge();
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == 0);

        registerSingleUpdateTxn(-5, i2, swift2);
        merge();
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == -5);
    }

    @Test
    public void prune1() {
        registerSingleUpdateTxn(1, i1, swift1);
        CausalityClock c = swift1.beginTxn().getClock();

        swift1.prune(i1, c);
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == 1);

    }

    @Test
    public void prune2() {
        registerSingleUpdateTxn(1, i1, swift1);
        CausalityClock c = swift1.beginTxn().getClock();
        registerSingleUpdateTxn(2, i1, swift1);

        swift1.prune(i1, c);
        // TesterUtils.printInformtion(i1, swift1.beginTxn());
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == 3);
    }

    @Test(expected = IllegalStateException.class)
    public void prune3() {
        for (int i = 0; i < 5; i++) {
            registerSingleUpdateTxn(1, i1, swift1);
        }
        CausalityClock c1 = swift1.beginTxn().getClock();

        for (int i = 0; i < 5; i++) {
            registerSingleUpdateTxn(1, i1, swift1);
        }
        CausalityClock c2 = swift1.beginTxn().getClock();

        for (int i = 0; i < 5; i++) {
            registerSingleUpdateTxn(1, i1, swift1);
        }

        swift1.prune(i1, c2);
        TesterUtils.printInformtion(i1, swift1.beginTxn());
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == 15);

        i1.getTxnLocalCopy(c1, swift2.beginTxn());
    }

    @Test
    public void mergePruned1() {
        for (int i = 0; i < 5; i++) {
            registerSingleUpdateTxn(1, i1, swift1);
        }

        for (int i = 0; i < 5; i++) {
            registerSingleUpdateTxn(1, i2, swift2);
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
            registerSingleUpdateTxn(1, i1, swift1);
        }
        CausalityClock c1 = swift1.beginTxn().getClock();
        swift1.prune(i1, c1);

        for (int i = 0; i < 5; i++) {
            registerSingleUpdateTxn(1, i2, swift2);
        }
        swift1.merge(i1, i2, swift2);

        // TesterUtils.printInformtion(i1, swift1.beginTxn());
        int result = getTxnLocal(i1, swift1.beginTxn()).getValue();
        assertTrue(result == 10);
    }

    @Test
    public void mergePruned3() {
        for (int i = 0; i < 5; i++) {
            registerSingleUpdateTxn(1, i1, swift1);
        }
        CausalityClock c1 = swift1.beginTxn().getClock();
        for (int i = 0; i < 5; i++) {
            registerSingleUpdateTxn(1, i1, swift1);
        }
        swift1.prune(i1, c1);

        for (int i = 0; i < 5; i++) {
            registerSingleUpdateTxn(1, i2, swift2);
        }

        CausalityClock c2 = swift2.beginTxn().getClock();
        for (int i = 0; i < 5; i++) {
            registerSingleUpdateTxn(1, i2, swift2);
        }
        swift2.prune(i2, c2);
        swift1.merge(i1, i2, swift2);

        // TesterUtils.printInformtion(i1, swift1.beginTxn());
        int result = getTxnLocal(i1, swift1.beginTxn()).getValue();
        assertTrue(result == 20);
    }

    @Test
    public void testTimestampsInUse() {
        final CausalityClock updatesSince = i1.getClock().clone();
        assertTrue(i1.getUpdatesTimestampMappingsSince(updatesSince).isEmpty());

        registerSingleUpdateTxn(1, i1, swift1);
        assertEquals(1, i1.getUpdatesTimestampMappingsSince(updatesSince).size());
    }

    @Test
    public void mergeIdemPotent() {
        registerSingleUpdateTxn(10, i1, swift1);
        IntegerVersioned icopy = i1.copy();
        swift1.merge(i1, icopy, swift1);
        int result = getTxnLocal(i1, swift1.beginTxn()).getValue();
        assertTrue(result == 10);
    }
}
