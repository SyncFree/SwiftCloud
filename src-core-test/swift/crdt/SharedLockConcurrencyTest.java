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

public class SharedLockConcurrencyTest {
    private ManagedCRDT<SharedLockCRDT> lock1, lock2;
    private String siteA = "A", siteB = "B", siteC = "C";
    private TxnTester txn;
    private SwiftTester swift1, swift2;

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

    private boolean registerReleaseTxn(String siteId, LockType type, ManagedCRDT<SharedLockCRDT> c, SwiftTester swift) {
        final TxnTester txn = swift.beginTxn(c);
        boolean result = false;
        try {
            final SharedLockCRDT lock = txn.get(c.getUID(), false, SharedLockCRDT.class);
            result = lock.releaseOwnership(siteId, type);
        } catch (WrongTypeException e) {
            throw new RuntimeException(e);
        } catch (NoSuchObjectException e) {
            throw new RuntimeException(e);
        } finally {
            txn.commit();
        }
        return result;
    }

    private boolean registerLockTxn(String parentId, LockType type, ManagedCRDT<SharedLockCRDT> c, SwiftTester swift) {
        final TxnTester txn = swift.beginTxn(c);
        boolean result = false;
        try {
            final SharedLockCRDT lock = txn.get(c.getUID(), false, SharedLockCRDT.class);
            result = lock.lock(parentId, type);
        } catch (WrongTypeException e) {
            throw new RuntimeException(e);
        } catch (NoSuchObjectException e) {
            throw new RuntimeException(e);
        } finally {
            txn.commit();
        }
        return result;
    }

    private boolean registerUnlockTxn(String parentId, LockType type, ManagedCRDT<SharedLockCRDT> c, SwiftTester swift) {
        final TxnTester txn = swift.beginTxn(c);
        boolean result = false;
        try {
            final SharedLockCRDT lock = txn.get(c.getUID(), false, SharedLockCRDT.class);
            result = lock.unlock(parentId, type);
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
        lock1 = new ManagedCRDT<SharedLockCRDT>(id, new SharedLockCRDT(id, siteA), ClockFactory.newClock(), true);
        swift1 = new SwiftTester("client1");
        lock2 = new ManagedCRDT<SharedLockCRDT>(id, new SharedLockCRDT(id, siteA), ClockFactory.newClock(), true);
        swift2 = new SwiftTester("client2");
    }

    @Test
    public void requestReceiveTest() {
        assertEquals(registerGrantTxn(siteA, siteB, LockType.WRITE_EXCLUSIVE, lock1, swift1), true);
        assertEquals(registerGrantTxn(siteA, siteB, LockType.WRITE_SHARED, lock1, swift1), false);
        swift2.merge(lock2, lock1, swift1);
        assertEquals(getLatestVersion(lock2, swift2.beginTxn()).getValue(), LockType.WRITE_EXCLUSIVE);
        assertEquals(registerReleaseTxn(siteA, LockType.WRITE_SHARED, lock2, swift2), false);
        assertEquals(registerReleaseTxn(siteB, LockType.WRITE_EXCLUSIVE, lock2, swift2), true);
        swift1.merge(lock1, lock2, swift2);
        assertEquals(getLatestVersion(lock1, swift1.beginTxn()).getValue(), LockType.WRITE_SHARED);

    }

    @Test
    public void SharedLockWithMergeTest() {
        assertEquals(registerGrantTxn(siteA, siteB, LockType.READ_SHARED, lock1, swift1), true);
        swift2.merge(lock2, lock1, swift1);
        assertEquals(registerLockTxn(siteB, LockType.READ_SHARED, lock2, swift2), true);
    }

    @Test
    public void SharedLockReleaseMergeTest() {
        assertEquals(registerGrantTxn(siteA, siteB, LockType.READ_SHARED, lock1, swift1), true);
        swift2.merge(lock2, lock1, swift1);
        assertEquals(registerLockTxn(siteB, LockType.READ_SHARED, lock2, swift2), true);
        assertEquals(registerReleaseTxn(siteB, LockType.READ_SHARED, lock2, swift2), false);
        assertEquals(registerUnlockTxn(siteA, LockType.READ_SHARED, lock1, swift1), false);
        assertEquals(registerUnlockTxn(siteB, LockType.READ_SHARED, lock2, swift2), true);
        assertEquals(registerReleaseTxn(siteB, LockType.READ_SHARED, lock2, swift2), true);
        assertEquals(registerGrantTxn(siteB, siteB, LockType.WRITE_SHARED, lock1, swift1), false);
        assertEquals(registerGrantTxn(siteA, siteB, LockType.WRITE_SHARED, lock1, swift1), false);
        swift1.merge(lock1, lock2, swift2);
        assertEquals(registerGrantTxn(siteA, siteB, LockType.WRITE_SHARED, lock1, swift1), true);

    }

}
