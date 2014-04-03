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

public class LowerBoundCounterTest {
    private static final String idA = "A", idB = "B";
    private TxnHandle txn;
    private LowerBoundCounterCRDT counter;
    private int initialValue = 10;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws SwiftException {
        txn = TxnTester.createIsolatedTxnTester();
        counter = txn.get(new CRDTIdentifier("A", "Int"), true, LowerBoundCounterCRDT.class);
    }

    @Test
    public void initTest() {
        assertEquals((int) counter.getValue(), 0);
    }

    @Test
    public void initValueTest() {
        counter = new LowerBoundCounterCRDT(new CRDTIdentifier("A", "Int"), initialValue);
        assertEquals((int) counter.getValue(), initialValue);
    }

    @Test
    public void incrementTest() {
        counter.increment(1, idA);
        assertEquals((int) counter.getValue(), 1);
        counter.increment(5, idA);
        assertEquals((int) counter.getValue(), 1 + 5);
    }

    @Test
    public void decrementTest() {
        assertEquals(counter.decrement(1, idA), false);
    }

    @Test
    public void incrementDecrementTest() {
        counter.increment(1, idA);
        assertEquals(counter.decrement(1, idA), true);
    }

    @Test
    public void incrementDecrementTest2() {
        counter.increment(5, idA);
        assertEquals(counter.decrement(5, idA), true);
    }

    @Test
    public void incrementTransferDecrementTest() {
        counter.increment(5, idA);
        assertEquals(counter.transfer(6, idA, idB), false);
        counter.transfer(5, idA, idB);
        assertEquals(counter.decrement(1, idA), false);
    }

    // Check never less than zero
    @After
    public void nonNegativeTest() {
        assertTrue((int) counter.getValue() >= 0);
    }

}
