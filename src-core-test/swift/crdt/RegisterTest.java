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
import swift.crdt.interfaces.TxnHandle;
import swift.exceptions.SwiftException;

public class RegisterTest {
    TxnHandle txn;
    RegisterTxnLocal<IntegerWrap> i;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws SwiftException {
        txn = new TxnTester("client1", ClockFactory.newClock());
        i = (RegisterTxnLocal<IntegerWrap>) txn.get(new CRDTIdentifier("A", "Int"), true, RegisterVersioned.class);
    }

    @Test
    public void initTest() {
        assertTrue(i.getValue() == null);
    }

    @Test
    public void setTest() {
        final int incr = 10;
        i.set(new IntegerWrap(incr));
        assertTrue(incr == i.getValue().i);
    }

    @Test
    public void getAndSetTest() {
        final int iterations = 5;
        for (int j = 0; j < iterations; j++) {
            i.set(new IntegerWrap(j));
            assertTrue(j == i.getValue().i);
        }
    }
}
