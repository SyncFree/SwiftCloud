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

import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;
import swift.exceptions.SwiftException;

public class AddWinsSetTest {
    TxnHandle txn;
    AddWinsSetCRDT<Integer> i;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws SwiftException {
        txn = TxnTester.createIsolatedTxnTester();
        i = txn.get(new CRDTIdentifier("A", "Int"), true, AddWinsSetCRDT.class);
    }

    @Test
    public void initTest() {
        assertTrue(i.getValue().isEmpty());
    }

    @Test
    public void emptyTest() {
        // lookup on empty set
        assertTrue(!i.lookup(0));
        assertTrue(i.getValue().isEmpty());
    }

    @Test
    public void insertTest() {
        int v = 5;
        int w = 7;
        // insert one element
        i.add(v);
        assertTrue(i.lookup(v));
        assertTrue(!i.lookup(w));

        // insertion should be idempotent
        i.add(v);
        assertTrue(i.lookup(v));
    }

    @Test
    public void deleteTest() {
        int v = 5;
        int w = 7;
        i.add(v);
        i.add(w);

        i.remove(v);
        assertTrue(!i.lookup(v));
        assertTrue(i.lookup(w));

        // remove should be idempotent
        i.remove(v);
        assertTrue(!i.lookup(v));
        assertTrue(i.lookup(w));

        i.remove(w);
        assertTrue(!i.lookup(v));
        assertTrue(!i.lookup(w));
    }
}
