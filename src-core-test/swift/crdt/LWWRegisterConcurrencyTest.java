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

import swift.clocks.ClockFactory;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.ManagedCRDT;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

// TODO: tests for concurrent RegisterUpdates or sequential RegisterUpdates with some interleavings.
public class LWWRegisterConcurrencyTest {
    ManagedCRDT<LWWRegisterCRDT<Integer>> i1, i2;
    SwiftTester swift1, swift2;
    static String[] CLIENT_IDS = new String[] { "client1", "client2" };

    private void merge() {
        swift1.merge(i1, i2, swift2);
    }

    private void registerSingleUpdateTxn(int value, ManagedCRDT<LWWRegisterCRDT<Integer>> i, SwiftTester swift) {
        final TxnTester txn = swift.beginTxn(i);
        try {
            @SuppressWarnings("unchecked")
            LWWRegisterCRDT<Integer> register = txn.get(i.getUID(), false, LWWRegisterCRDT.class);
            register.set(new Integer(value));
        } catch (WrongTypeException e) {
            throw new RuntimeException(e);
        } catch (NoSuchObjectException e) {
            throw new RuntimeException(e);
        }
        txn.commit();
    }

    @Before
    public void setUp() throws WrongTypeException, NoSuchObjectException, VersionNotFoundException {
        final CRDTIdentifier id = new CRDTIdentifier("a", "a");
        i1 = new ManagedCRDT<LWWRegisterCRDT<Integer>>(id, new LWWRegisterCRDT<Integer>(id), ClockFactory.newClock(),
                true);
        i2 = new ManagedCRDT<LWWRegisterCRDT<Integer>>(id, new LWWRegisterCRDT<Integer>(id), ClockFactory.newClock(),
                true);
        swift1 = new SwiftTester(CLIENT_IDS[0]);
        swift2 = new SwiftTester(CLIENT_IDS[1]);
    }

    // Merge with empty set
    @Test
    public void mergeEmpty1() {
        registerSingleUpdateTxn(5, i1, swift1);
        merge();

        assertEquals(new Integer(5), i1.getLatestVersion(swift1.beginTxn()).getValue());
    }

    // Merge with empty set
    @Test
    public void mergeEmpty2() {
        registerSingleUpdateTxn(5, i2, swift2);
        i1.merge(i2);
        assertEquals(new Integer(5), i1.getLatestVersion(swift1.beginTxn()).getValue());
    }

    @Test
    public void mergeNonEmpty() {
        registerSingleUpdateTxn(5, i1, swift1);
        registerSingleUpdateTxn(6, i2, swift2);
        i1.merge(i2);
        assertTrue(i1.getLatestVersion(swift1.beginTxn()).getValue().equals(6)
                || i1.getLatestVersion(swift1.beginTxn()).getValue().equals(5));
    }

    @Test
    public void mergeMultiple() {
        registerSingleUpdateTxn(5, i2, swift2);
        registerSingleUpdateTxn(6, i2, swift2);
        i1.merge(i2);
        assertTrue(i1.getLatestVersion(swift1.beginTxn()).getValue().equals(6));
    }

    @Test
    public void mergeConcurrentAddRem() {
        registerSingleUpdateTxn(5, i1, swift1);
        registerSingleUpdateTxn(-5, i2, swift2);
        merge();
        assertTrue(i1.getLatestVersion(swift1.beginTxn()).getValue().equals(-5)
                || i1.getLatestVersion(swift1.beginTxn()).getValue().equals(5));

        registerSingleUpdateTxn(2, i1, swift1);
        assertTrue(i1.getLatestVersion(swift1.beginTxn()).getValue().equals(2));
    }

    @Test
    public void mergeConcurrentCausal() {
        registerSingleUpdateTxn(1, i1, swift1);
        registerSingleUpdateTxn(-1, i1, swift1);
        registerSingleUpdateTxn(2, i2, swift2);
        merge();
        assertTrue(i1.getLatestVersion(swift1.beginTxn()).getValue().equals(2)
                || i1.getLatestVersion(swift1.beginTxn()).getValue().equals(-1));
    }

    @Test
    public void mergeConcurrentCausal2() {
        registerSingleUpdateTxn(1, i1, swift1);
        registerSingleUpdateTxn(-1, i1, swift1);
        registerSingleUpdateTxn(2, i2, swift2);
        registerSingleUpdateTxn(-2, i2, swift2);
        merge();
        assertTrue(i1.getLatestVersion(swift1.beginTxn()).getValue().equals(-1)
                || i1.getLatestVersion(swift1.beginTxn()).getValue().equals(-2));

        registerSingleUpdateTxn(-5, i2, swift2);
        merge();
        assertTrue(i1.getLatestVersion(swift1.beginTxn()).getValue().equals(-1)
                || i1.getLatestVersion(swift1.beginTxn()).getValue().equals(-5));
    }
}
