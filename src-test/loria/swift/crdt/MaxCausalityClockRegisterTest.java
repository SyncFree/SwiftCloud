/*
 *  Replication Benchmarker
 *  https://github.com/score-team/replication-benchmarker/
 *  Copyright (C) 2012 LORIA / Inria / SCORE Team
 * 
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 * 
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 * 
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.

 */
package loria.swift.crdt;

import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;
import swift.clocks.CausalityClock;
import swift.clocks.ClockFactory;
import swift.clocks.IncrementalTimestampGenerator;
import swift.clocks.Timestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.IntegerWrap;
import swift.crdt.RegisterTxnLocal;
import swift.crdt.RegisterVersioned;
import swift.crdt.TxnTester;
import swift.crdt.interfaces.TxnHandle;
import swift.exceptions.SwiftException;

/**
 *
 * @author Stephane Martin <stephane.martin@loria.fr>
 */
public class MaxCausalityClockRegisterTest {
    
    public MaxCausalityClockRegisterTest() {
    }

    TxnHandle txn;
    MaxCausalityClockTxnLocal i;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws SwiftException {
        txn = new TxnTester("client1", ClockFactory.newClock());
        i = (MaxCausalityClockTxnLocal) txn.get(new CRDTIdentifier("A", "Max"), true, (Class)MaxCausalityClockRegister.class);
    }

    @Test
    public void initTest() {
        assertEquals(i.getValue(),ClockFactory.newClock());
    }

    @Test
    public void setTest() {
        CausalityClock clock = ClockFactory.newClock();
        IncrementalTimestampGenerator siteCGen = new IncrementalTimestampGenerator("c");
        IncrementalTimestampGenerator siteDGen = new IncrementalTimestampGenerator("d");
    
        Timestamp tsC1 = siteCGen.generateNew();
        Timestamp tsC2 = siteCGen.generateNew();
        Timestamp tsD1 = siteDGen.generateNew();
        Timestamp tsD2 = siteDGen.generateNew();
        
        clock.record(tsD2);
        clock.record(tsC1);
        clock.record(tsD1);
        clock.record(tsC2);
        
        i.setValue(clock);
        assertEquals(clock,i.getValue());
    }

    @Test
    public void getAndSetTest() {
        final int iterations = 5;
        IncrementalTimestampGenerator [] siteGen = {new IncrementalTimestampGenerator("c"),new IncrementalTimestampGenerator("d")};
        CausalityClock clock =ClockFactory.newClock();
        for (int j = 0; j < iterations; j++) {
            clock = (CausalityClock)clock.copy();
            clock.includes(siteGen[j%siteGen.length].generateNew());
            i.setValue(clock);
            assertEquals(clock ,i.getValue());
        }
    }
}
