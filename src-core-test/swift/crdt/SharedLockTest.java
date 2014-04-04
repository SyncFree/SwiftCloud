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

public class SharedLockTest {
    private ManagedCRDT<SharedLockCRDT> lock;
    private String siteA = "A", siteB = "B", siteC = "C";
    private TxnTester txn;
    private SwiftTester swift1;

    private SharedLockCRDT getLatestVersion(ManagedCRDT<SharedLockCRDT> l, TxnTester txn) {
        return l.getVersion(l.getClock(), txn);
    }

    private boolean registerGrantTxn(String parentId, String siteId, LockType type, ManagedCRDT<SharedLockCRDT> c,
            SwiftTester swift) {
        final TxnTester txn = swift.beginTxn(c);
        boolean result = false;
        try {
            final SharedLockCRDT lock = txn.get(c.getUID(), false, SharedLockCRDT.class);
            result = lock.getOwnership(parentId, siteId, type);
        } catch (WrongTypeException e) {
            throw new RuntimeException(e);
        } catch (NoSuchObjectException e) {
            throw new RuntimeException(e);
        } finally {
            txn.commit();
        }
        return result;
    }

    private boolean registerReleaseTxn(String parentId, String siteId, LockType type, ManagedCRDT<SharedLockCRDT> c,
            SwiftTester swift) {
        final TxnTester txn = swift.beginTxn(c);
        boolean result = false;
        try {
            final SharedLockCRDT lock = txn.get(c.getUID(), false, SharedLockCRDT.class);
            result = lock.releaseOwnership(parentId, siteId, type);
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
        lock = new ManagedCRDT<SharedLockCRDT>(id, new SharedLockCRDT(id, siteA), ClockFactory.newClock(), true);
        swift1 = new SwiftTester("client1");
    }

    @Test
    public void initTest() {
        assertEquals(getLatestVersion(lock, swift1.beginTxn()).getValue(), LockType.WRITE_SHARED);
    }

    @Test
    public void requestReleaseExclusiveTest() {
        assertEquals(registerGrantTxn(siteA, siteA, LockType.WRITE_EXCLUSIVE, lock, swift1), true);
        assertEquals(registerReleaseTxn(siteA, siteA, LockType.WRITE_EXCLUSIVE, lock, swift1), true);
        assertEquals(getLatestVersion(lock, swift1.beginTxn()).getValue(), LockType.WRITE_SHARED);
    }

    @Test
    public void requestReleaseExclusiveFailTest() {
        assertEquals(registerGrantTxn(siteA, siteB, LockType.WRITE_EXCLUSIVE, lock, swift1), true);
        assertEquals(registerReleaseTxn(siteA, siteB, LockType.WRITE_EXCLUSIVE, lock, swift1), false);
        assertEquals(getLatestVersion(lock, swift1.beginTxn()).getValue(), LockType.WRITE_EXCLUSIVE);
    }

    @Test
    public void SharedToExclusiveTest() {
        assertEquals(registerGrantTxn(siteA, siteB, LockType.WRITE_EXCLUSIVE, lock, swift1), true);
        assertEquals(registerGrantTxn(siteA, siteC, LockType.READ_SHARED, lock, swift1), false);
    }

    @Test
    public void SharedToSharedFailTest() {
        assertEquals(registerGrantTxn(siteA, siteB, LockType.WRITE_SHARED, lock, swift1), true);
        assertEquals(registerGrantTxn(siteA, siteB, LockType.READ_SHARED, lock, swift1), false);
        assertEquals(registerGrantTxn(siteA, siteA, LockType.READ_SHARED, lock, swift1), false);

    }

}
