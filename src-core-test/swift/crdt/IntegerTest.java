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

public class IntegerTest {
    TxnHandle txn;
    IntegerCRDT i;

    @Before
    public void setUp() throws SwiftException {
        txn = TxnTester.createIsolatedTxnTester();
        i = txn.get(new CRDTIdentifier("A", "Int"), true, IntegerCRDT.class);
    }

    @Test
    public void initTest() {
        assertTrue(i.getValue() == 0);
    }

    @Test
    public void addTest() {
        final int incr = 10;
        i.add(incr);
        assertTrue(incr == i.getValue());
    }

    @Test
    public void addTest2() {
        final int incr = 10;
        for (int j = 0; j < incr; j++) {
            i.add(1);
        }
        assertTrue(incr == i.getValue());
    }

    @Test
    public void subTest() {
        final int decr = 10;
        i.sub(decr);
        assertTrue(decr == -i.getValue());
    }

    @Test
    public void subTest2() {
        final int decr = 10;
        for (int j = 0; j < decr; j++) {
            i.sub(1);
        }
        assertTrue(decr == -i.getValue());
    }

    @Test
    public void addAndSubTest() {
        final int incr = 10;
        final int iterations = 5;
        for (int j = 0; j < iterations; j++) {
            i.add(incr);
            i.sub(incr);
            assertTrue(0 == i.getValue());
        }
    }
}
