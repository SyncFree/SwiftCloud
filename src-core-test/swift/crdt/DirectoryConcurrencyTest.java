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

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import swift.clocks.ClockFactory;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.ManagedCRDT;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

/**
 * @author annettebieniusa
 * 
 */
public class DirectoryConcurrencyTest {
    SwiftTester swift1, swift2;
    ManagedCRDT<DirectoryCRDT> i1, i2;
    String dirTable = "DIR";

    @Before
    public void setUp() throws WrongTypeException, NoSuchObjectException, VersionNotFoundException {
        final CRDTIdentifier id = new CRDTIdentifier(dirTable, DirectoryCRDT.getDirEntry("/root", DirectoryCRDT.class));
        i1 = new ManagedCRDT<DirectoryCRDT>(id, new DirectoryCRDT(id), ClockFactory.newClock(), true);
        i2 = new ManagedCRDT<DirectoryCRDT>(id, new DirectoryCRDT(id), ClockFactory.newClock(), true);

        swift1 = new SwiftTester("client1");
        swift2 = new SwiftTester("client2");
    }

    private void mergeI2IntoI1() {
        swift1.merge(i1, i2, swift2);
    }

    private DirectoryCRDT getLatestVersion(ManagedCRDT<DirectoryCRDT> i, TxnTester txn) {
        return i.getVersion(i.getClock(), txn);
    }

    private void registerSingleCreateTxn(String entry, ManagedCRDT<DirectoryCRDT> i, SwiftTester swift) {
        final TxnTester txn = swift.beginTxn(i);
        try {
            DirectoryCRDT dir = txn.get(i.getUID(), false, DirectoryCRDT.class);
            dir.createNewEntry(entry, DirectoryCRDT.class);
        } catch (WrongTypeException e) {
            throw new RuntimeException(e);
        } catch (NoSuchObjectException e) {
            throw new RuntimeException(e);
        } catch (VersionNotFoundException e) {
            throw new RuntimeException(e);
        } catch (NetworkException e) {
            throw new RuntimeException(e);
        } finally {
            // TODO Q: is this "false" anything important?
            txn.commit(false);
        }
    }

    private void registerSingleRemoveTxn(String entry, ManagedCRDT<DirectoryCRDT> i, SwiftTester swift) {
        final TxnTester txn = swift.beginTxn(i);
        DirectoryCRDT dir;
        try {
            dir = txn.get(i.getUID(), false, DirectoryCRDT.class);
            dir.removeEntry(entry, DirectoryCRDT.class);
        } catch (WrongTypeException e) {
            throw new RuntimeException(e);
        } catch (NoSuchObjectException e) {
            throw new RuntimeException(e);
        } catch (VersionNotFoundException e) {
            throw new RuntimeException(e);
        } catch (NetworkException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        txn.commit();
    }

    // Merge with empty set
    @Test
    public void mergeEmpty1() {
        registerSingleCreateTxn("x", i1, swift1);
        mergeI2IntoI1();
        assertTrue(getLatestVersion(i1, swift1.beginTxn()).contains("x", DirectoryCRDT.class));
    }

    // Merge with empty set
    @Test
    public void mergeEmpty2() {
        registerSingleCreateTxn("x", i2, swift2);
        mergeI2IntoI1();
        assertTrue(getLatestVersion(i1, swift1.beginTxn()).contains("x", DirectoryCRDT.class));
    }

    @Test
    public void mergeNonEmpty() {
        registerSingleCreateTxn("x", i1, swift1);
        registerSingleCreateTxn("y", i2, swift2);
        mergeI2IntoI1();
        assertTrue(getLatestVersion(i1, swift1.beginTxn()).contains("x", DirectoryCRDT.class));
        assertTrue(getLatestVersion(i1, swift1.beginTxn()).contains("y", DirectoryCRDT.class));
    }

    @Test
    public void mergeConcurrentInsert() {
        registerSingleCreateTxn("x", i1, swift1);
        registerSingleCreateTxn("x", i2, swift2);
        mergeI2IntoI1();
        assertTrue(getLatestVersion(i1, swift1.beginTxn()).contains("x", DirectoryCRDT.class));
    }

    @Test
    public void mergeConcurrentRemove() {
        registerSingleRemoveTxn("x", i1, swift1);
        registerSingleRemoveTxn("x", i2, swift2);
        mergeI2IntoI1();
        assertTrue(!getLatestVersion(i1, swift1.beginTxn()).contains("x", DirectoryCRDT.class));
    }

    @Test
    public void mergeConcurrentAddRem() {
        registerSingleCreateTxn("x", i1, swift1);

        registerSingleRemoveTxn("x", i2, swift2);
        mergeI2IntoI1();
        assertTrue(getLatestVersion(i1, swift1.beginTxn()).contains("x", DirectoryCRDT.class));

        registerSingleRemoveTxn("x", i1, swift1);
        assertTrue(!getLatestVersion(i1, swift1.beginTxn()).contains("x", DirectoryCRDT.class));
    }

    @Test
    public void mergeConcurrentCausal() {
        registerSingleCreateTxn("x", i1, swift1);
        registerSingleRemoveTxn("x", i1, swift1);

        registerSingleCreateTxn("x", i2, swift2);
        mergeI2IntoI1();
        assertTrue(getLatestVersion(i1, swift1.beginTxn()).contains("x", DirectoryCRDT.class));
    }
}
