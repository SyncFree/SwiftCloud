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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.SwiftException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

public class DirectoryTest {
    TxnHandle txn;
    DirectoryCRDT dir;

    @Before
    public void setUp() throws SwiftException {
        txn = TxnTester.createIsolatedTxnTester();
        dir = txn.get(DirectoryCRDT.createRootId("DIR", "root", DirectoryCRDT.class), true, DirectoryCRDT.class);
    }

    @Test
    public void idTest() {
        assertEquals(dir.getUID(), new CRDTIdentifier("DIR", "/root:swift.crdt.DirectoryCRDT"));
    }

    @Test
    public void emptyTest() {
        // lookup on empty set
        assertTrue(!dir.contains("x", IntegerCRDT.class));
        assertTrue(dir.getValue().isEmpty());
    }

    @Test
    public void insertTest() throws WrongTypeException, NoSuchObjectException, VersionNotFoundException,
            NetworkException, ClassNotFoundException {
        // create one element
        dir.createNewEntry("x", IntegerCRDT.class);
        dir = txn.get(DirectoryCRDT.createRootId("DIR", "root", DirectoryCRDT.class), false, DirectoryCRDT.class);
        IntegerCRDT i2 = dir.get("x", IntegerCRDT.class);
        assertTrue(i2.getValue() == 0);
        assertTrue(dir.contains("x", IntegerCRDT.class));
    }

    @Test
    public void removeTest() throws WrongTypeException, NoSuchObjectException, VersionNotFoundException,
            NetworkException, ClassNotFoundException {
        // create one element
        dir.createNewEntry("x", IntegerCRDT.class);
        dir = txn.get(DirectoryCRDT.createRootId("DIR", "root", DirectoryCRDT.class), false, DirectoryCRDT.class);
        assertTrue(dir.contains("x", IntegerCRDT.class));

        dir.removeEntry("x", IntegerCRDT.class);
        dir = txn.get(DirectoryCRDT.createRootId("DIR", "root", DirectoryCRDT.class), false, DirectoryCRDT.class);
        assertFalse(dir.contains("x", IntegerCRDT.class));
    }

    @Test
    public void removeRecursiveTest() throws WrongTypeException, NoSuchObjectException, VersionNotFoundException,
            NetworkException, ClassNotFoundException {
        DirectoryCRDT parent = dir;
        for (int i = 0; i < 5; i++) {
            CRDTIdentifier childDirId = parent.createNewEntry("x" + i, DirectoryCRDT.class);
            parent.createNewEntry("y" + i, DirectoryCRDT.class);
            parent.createNewEntry("z" + i, IntegerCRDT.class);

            DirectoryCRDT child = txn.get(childDirId, false, DirectoryCRDT.class);
            parent = child;
        }
        dir.removeEntry("x" + 0, DirectoryCRDT.class);
        dir = txn.get(DirectoryCRDT.createRootId("DIR", "root", DirectoryCRDT.class), false, DirectoryCRDT.class);
        assertFalse(dir.contains("x" + 0, DirectoryCRDT.class));
        assertTrue(dir.contains("y" + 0, DirectoryCRDT.class));

        // Test directly one of the subdirectories to be empty
        CRDTIdentifier subdirId = new CRDTIdentifier("DIR", "/root/x0/x1/x2/x3/x4:swift.crdt.DirectoryCRDT");
        DirectoryCRDT subdir = txn.get(subdirId, false, DirectoryCRDT.class);
        assertTrue(subdir.getValue().isEmpty());
    }

    @Test
    public void differentEntryTypesTest() throws WrongTypeException, NoSuchObjectException, VersionNotFoundException,
            NetworkException, ClassNotFoundException {
        dir.createNewEntry("x", IntegerCRDT.class);
        dir = txn.get(DirectoryCRDT.createRootId("DIR", "root", DirectoryCRDT.class), false, DirectoryCRDT.class);
        assertTrue(dir.contains("x", IntegerCRDT.class));
        dir.createNewEntry("x", DirectoryCRDT.class);
        dir = txn.get(DirectoryCRDT.createRootId("DIR", "root", DirectoryCRDT.class), false, DirectoryCRDT.class);
        assertTrue(dir.contains("x", DirectoryCRDT.class));

        dir.removeEntry("x", IntegerCRDT.class);
        dir = txn.get(DirectoryCRDT.createRootId("DIR", "root", DirectoryCRDT.class), false, DirectoryCRDT.class);
        assertFalse(dir.contains("x", IntegerCRDT.class));
        assertTrue(dir.contains("x", DirectoryCRDT.class));
    }

    @Test
    public void removeAndReconstructTest() throws WrongTypeException, NoSuchObjectException, VersionNotFoundException,
            NetworkException, ClassNotFoundException {
        DirectoryCRDT parent = dir;
        for (int i = 0; i < 5; i++) {
            CRDTIdentifier childDirId = parent.createNewEntry("x" + i, DirectoryCRDT.class);
            parent.createNewEntry("y" + i, DirectoryCRDT.class);

            DirectoryCRDT child = txn.get(childDirId, false, DirectoryCRDT.class);
            parent = child;
        }
        dir = txn.get(DirectoryCRDT.createRootId("DIR", "root", DirectoryCRDT.class), false, DirectoryCRDT.class);
        dir.removeEntry("x" + 0, DirectoryCRDT.class);

        parent.createNewEntry("z", DirectoryCRDT.class);
        parent = txn.get(DirectoryCRDT.createRootId("DIR", "root", DirectoryCRDT.class), false, DirectoryCRDT.class);
        for (int i = 0; i < 5; i++) {
            assertTrue(parent.contains("x" + i, DirectoryCRDT.class));
            if (i > 0) {
                assertFalse(parent.contains("y" + i, DirectoryCRDT.class));
            }
            CRDTIdentifier childDirId = new CRDTIdentifier("DIR", DirectoryCRDT.getDirEntry(
                    DirectoryCRDT.getFullPath(parent.getUID()) + "/x" + i, DirectoryCRDT.class));
            DirectoryCRDT child = txn.get(childDirId, false, DirectoryCRDT.class);
            parent = child;
        }
    }
}
