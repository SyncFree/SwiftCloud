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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;
import swift.exceptions.SwiftException;

public class UpperBoundCounterTest {
    private static final String idA = "A", idB = "B";
    private TxnHandle txn;
    private UpperBoundCounterCRDT counter;
    private int initialValue = 10;

    @Before
    public void setUp() throws SwiftException {
        txn = TxnTester.createIsolatedTxnTester();
        counter = txn.get(new CRDTIdentifier("A", "Int"), true, UpperBoundCounterCRDT.class);
    }

    @Test
    public void initTest() {
        assertEquals((int) counter.getValue(), 0);
    }

    @Test
    public void initValueTest() {
        counter = new UpperBoundCounterCRDT(new CRDTIdentifier("A", "Int"), initialValue);
        assertEquals((int) counter.getValue(), initialValue);
    }

    @Test
    public void decrementTest() {
        counter.decrement(1, idA);
        assertEquals((int) counter.getValue(), -1);
        counter.decrement(5, idA);
        assertEquals((int) counter.getValue(), -1 - 5);
    }

    @Test
    public void incrementTest() {
        assertEquals(counter.increment(1, idA), false);
    }

    @Test
    public void incrementDecrementTest() {
        counter.decrement(1, idA);
        assertEquals(counter.increment(1, idA), true);
    }

    @Test
    public void incrementDecrementTest2() {
        counter.decrement(5, idA);
        assertEquals(counter.increment(5, idA), true);
    }

    @Test
    public void incrementTransferDecrementTest() {
        counter.decrement(5, idA);
        assertEquals(counter.transfer(6, idA, idB), false);
        counter.transfer(5, idA, idB);
        assertEquals(counter.increment(1, idA), false);
    }

    // Check never less than zero
    @After
    public void nonNegativeTest() {
        assertTrue((int) counter.getValue() <= initialValue);
    }

}
