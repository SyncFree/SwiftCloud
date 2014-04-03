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

import org.junit.Before;
import org.junit.Test;

import swift.clocks.ClockFactory;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.ManagedCRDT;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

public class LowerBoundCounterConcurrencyTest {
    private static final String idA = "A", idB = "B";

    SwiftTester swift1, swift2;
    ManagedCRDT<LowerBoundCounterCRDT> counter1, counter2;

    private void mergeC2IntoC1() {
        swift1.merge(counter1, counter2, swift2);
    }

    private void mergeC1IntoC2() {
        swift2.merge(counter2, counter1, swift2);
    }

    private LowerBoundCounterCRDT getLatestVersion(ManagedCRDT<LowerBoundCounterCRDT> c, TxnTester txn) {
        return c.getVersion(c.getClock(), txn);
    }

    private boolean registerIncTxn(int value, String siteId, ManagedCRDT<LowerBoundCounterCRDT> c, SwiftTester swift) {
        final TxnTester txn = swift.beginTxn(c);
        boolean result = false;
        try {
            final LowerBoundCounterCRDT counter = txn.get(c.getUID(), false, LowerBoundCounterCRDT.class);
            result = counter.increment(value, siteId);
        } catch (WrongTypeException e) {
            throw new RuntimeException(e);
        } catch (NoSuchObjectException e) {
            throw new RuntimeException(e);
        } finally {
            txn.commit();
        }
        return result;
    }

    private boolean registerDecTxn(int value, String siteId, ManagedCRDT<LowerBoundCounterCRDT> c, SwiftTester swift) {
        final TxnTester txn = swift.beginTxn(c);
        boolean result = false;
        try {
            final LowerBoundCounterCRDT counter = txn.get(c.getUID(), false, LowerBoundCounterCRDT.class);
            result = counter.decrement(value, siteId);
        } catch (WrongTypeException e) {
            throw new RuntimeException(e);
        } catch (NoSuchObjectException e) {
            throw new RuntimeException(e);
        } finally {
            txn.commit();
        }
        return result;

    }

    private boolean registerTransferTxn(int value, String originId, String targetId,
            ManagedCRDT<LowerBoundCounterCRDT> c, SwiftTester swift) {
        final TxnTester txn = swift.beginTxn(c);
        boolean result = false;
        try {
            final LowerBoundCounterCRDT counter = txn.get(c.getUID(), false, LowerBoundCounterCRDT.class);
            result = counter.transfer(value, originId, targetId);
        } catch (WrongTypeException e) {
            throw new RuntimeException(e);
        } catch (NoSuchObjectException e) {
            throw new RuntimeException(e);
        } finally {
            txn.commit();
        }
        return result;
    }

    @Before
    public void setUp() throws WrongTypeException, NoSuchObjectException, VersionNotFoundException {
        final CRDTIdentifier id = new CRDTIdentifier("a", "a");
        counter1 = new ManagedCRDT<LowerBoundCounterCRDT>(id, new LowerBoundCounterCRDT(id), ClockFactory.newClock(),
                true);
        counter2 = new ManagedCRDT<LowerBoundCounterCRDT>(id, new LowerBoundCounterCRDT(id), ClockFactory.newClock(),
                true);
        swift1 = new SwiftTester("client1");
        swift2 = new SwiftTester("client2");
    }

    // Merge with empty set
    @Test
    public void mergeEmpty1() {
        registerIncTxn(5, idA, counter1, swift1);
        mergeC2IntoC1();
        assertEquals((int) getLatestVersion(counter1, swift1.beginTxn()).getValue(), 5);
    }

    // Merge with empty set
    @Test
    public void mergeEmpty2() {
        registerIncTxn(5, idB, counter2, swift1);
        mergeC2IntoC1();
        assertEquals((int) getLatestVersion(counter2, swift1.beginTxn()).getValue(), 5);
    }

    @Test
    public void mergeNonEmptyDiffOwner() {
        registerIncTxn(5, idA, counter1, swift1);
        registerIncTxn(6, idB, counter2, swift2);
        mergeC2IntoC1();
        assertEquals((int) getLatestVersion(counter1, swift1.beginTxn()).getValue(), 11);
    }

    @Test
    public void mergeNonEmptySameOwner() {
        registerIncTxn(5, idA, counter1, swift1);
        registerIncTxn(6, idA, counter2, swift2);
        mergeC2IntoC1();
        assertEquals((int) getLatestVersion(counter1, swift1.beginTxn()).getValue(), 11);
    }

    @Test
    public void mergeTransfer() {
        registerIncTxn(5, idA, counter1, swift1);
        assertEquals(registerDecTxn(5, idB, counter2, swift2), false);
        assertEquals(registerDecTxn(5, idB, counter2, swift2), false);
        registerTransferTxn(5, idA, idB, counter1, swift1);
        assertEquals(registerDecTxn(5, idA, counter1, swift1), false);
        assertEquals(registerDecTxn(5, idB, counter2, swift2), false);
        mergeC1IntoC2();
        assertEquals(registerDecTxn(5, idB, counter2, swift2), true);
        assertEquals((int) getLatestVersion(counter2, swift2.beginTxn()).getValue(), 0);
    }
}
