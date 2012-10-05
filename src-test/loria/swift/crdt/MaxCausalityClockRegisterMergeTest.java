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

import loria.swift.crdt.operation.MaxCausalityClockUpdate;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import swift.clocks.CausalityClock;
import swift.clocks.ClockFactory;
import swift.clocks.IncrementalTimestampGenerator;
import swift.crdt.SwiftTester;
import swift.crdt.TesterUtils;
import swift.crdt.TxnTester;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.Copyable;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

/**
 *
 * @author Stephane Martin <stephane.martin@loria.fr>
 */
public class MaxCausalityClockRegisterMergeTest {

    public MaxCausalityClockRegisterMergeTest() {
    }
    MaxCausalityClockRegister i1, i2;
    SwiftTester swift1, swift2;
    CausalityClock clocks[] = new CausalityClock[3];
    IncrementalTimestampGenerator[] siteGen = {new IncrementalTimestampGenerator("c"), new IncrementalTimestampGenerator("d")};
    CausalityClock tmpClk;
    CausalityClock tmpClk2;
    CausalityClock tmpClk3;
    CausalityClock tmpClk4;
    CausalityClock max;

    private <V extends Copyable> MaxCausalityClockTxnLocal getTxnLocal(MaxCausalityClockRegister i, TxnTester txn) {
        return (MaxCausalityClockTxnLocal) TesterUtils.getTxnLocal(i, txn);
    }

    private CausalityClock generateNew(int i) {
        int nsite = (i % siteGen.length);
        int nclock = (i / siteGen.length) % clocks.length;
        CausalityClock clock = clocks[nclock];
        clock.record(siteGen[nsite].generateNew());
        return clock;
    }

    private void merge() {
        swift1.merge((CRDT) i1, (CRDT) i2, swift2);
    }

    private void registerSingleUpdateTxn(CausalityClock value, MaxCausalityClockRegister i, SwiftTester swift) {
        final TxnTester txn = swift.beginTxn();
        registerUpdate(value, i, txn);
        txn.commit();
    }

    private void registerUpdate(CausalityClock value, MaxCausalityClockRegister i, TxnTester txn) {
        txn.registerOperation(i,
                new MaxCausalityClockUpdate(txn.nextTimestamp(),
                value,
                txn.getClock().clone()));
    }

    @Before
    public void setUp() throws WrongTypeException, NoSuchObjectException, VersionNotFoundException {
        i1 = new MaxCausalityClockRegister();
        i1.init(null, ClockFactory.newClock(), ClockFactory.newClock(), true);

        i2 = new MaxCausalityClockRegister();
        i2.init(null, ClockFactory.newClock(), ClockFactory.newClock(), true);
        swift1 = new SwiftTester("client1");
        swift2 = new SwiftTester("client2");

        for (int i = 0; i < clocks.length; i++) {
            clocks[i] = ClockFactory.newClock();
        }
        max = ClockFactory.newClock();
    }

    // Merge with empty set
    @Test
    public void mergeEmpty1() {
        tmpClk = generateNew(1);
        registerSingleUpdateTxn(tmpClk, i1, swift1);
        merge();

        assertEquals(getTxnLocal(i1, swift1.beginTxn()).getValue(), tmpClk);
    }

    // Merge with empty set
    @Test
    public void mergeEmpty2() {
        tmpClk = generateNew(3);
        registerSingleUpdateTxn(tmpClk, i2, swift2);
        i1.merge(i2);
        assertEquals(getTxnLocal(i1, swift1.beginTxn()).getValue(), tmpClk);
    }

    @Test
    public void mergeNonEmpty() {
        tmpClk = generateNew(3);
        tmpClk2 = generateNew(6);
        registerSingleUpdateTxn(tmpClk, i1, swift1);
        registerSingleUpdateTxn(tmpClk2, i2, swift2);
        i1.merge(i2);
        max.merge(tmpClk);
        max.merge(tmpClk2);
        assertEquals(getTxnLocal(i1, swift1.beginTxn()).getValue(), max);

    }

    @Test
    public void mergeMultiple() {
        tmpClk = generateNew(3);
        tmpClk2 = generateNew(6);
        registerSingleUpdateTxn(tmpClk, i1, swift1);
        registerSingleUpdateTxn(tmpClk2, i2, swift2);
        i1.merge(i2);
        max.merge(tmpClk);
        max.merge(tmpClk2);
        assertEquals(getTxnLocal(i1, swift1.beginTxn()).getValue(), max);
    }

    @Test
    public void mergeConcurrentAddRem() {
        tmpClk = generateNew(3);
        tmpClk2 = generateNew(6);
        tmpClk3 = generateNew(4);
        registerSingleUpdateTxn(tmpClk, i1, swift1);
        registerSingleUpdateTxn(tmpClk2, i2, swift2);
        merge();
        max.merge(tmpClk);
        max.merge(tmpClk2);
        assertEquals(getTxnLocal(i1, swift1.beginTxn()).getValue(), max);


        registerSingleUpdateTxn(tmpClk3, i1, swift1);
        max.merge(tmpClk3);

        assertEquals(getTxnLocal(i1, swift1.beginTxn()).getValue(), max);
    }

    @Test
    public void mergeConcurrentCausal() {
        tmpClk = generateNew(3);
        tmpClk2 = generateNew(6);
        tmpClk3 = generateNew(4);
        registerSingleUpdateTxn(tmpClk, i1, swift1);
        registerSingleUpdateTxn(tmpClk2, i1, swift1);
        registerSingleUpdateTxn(tmpClk3, i2, swift2);
        /*       registerSingleUpdateTxn(1, i1, swift1);
         registerSingleUpdateTxn(-1, i1, swift1);
         registerSingleUpdateTxn(2, i2, swift2);*/
        merge();
        max.merge(tmpClk);
        max.merge(tmpClk2);
        max.merge(tmpClk3);
        assertEquals(getTxnLocal(i1, swift1.beginTxn()).getValue(), max);
    }

    @Test
    public void mergeConcurrentCausal2() {
        tmpClk = generateNew(3);
        tmpClk2 = generateNew(6);
        tmpClk3 = generateNew(4);
        tmpClk4 = generateNew(9);
        registerSingleUpdateTxn(tmpClk, i1, swift1);
        registerSingleUpdateTxn(tmpClk2, i1, swift1);
        registerSingleUpdateTxn(tmpClk3, i2, swift2);
        registerSingleUpdateTxn(tmpClk4, i2, swift2);
        merge();
        max.merge(tmpClk);
        max.merge(tmpClk2);
        max.merge(tmpClk3);
        max.merge(tmpClk4);

        /* assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue().equals(tmpClk2)
         || getTxnLocal(i1, swift1.beginTxn()).getValue().equals(tmpClk3));*/
        assertEquals(getTxnLocal(i1, swift1.beginTxn()).getValue(), max);
        tmpClk = generateNew(2);
        registerSingleUpdateTxn(tmpClk, i2, swift2);
        merge();
        max.merge(tmpClk);

        assertEquals(getTxnLocal(i1, swift1.beginTxn()).getValue(), max);
    }

    @Test
    public void prune1() {
        tmpClk = generateNew(3);
        registerSingleUpdateTxn(tmpClk, i1, swift1);
        CausalityClock c = swift1.beginTxn().getClock();

        swift1.prune((CRDT) i1, c);
        assertEquals(getTxnLocal(i1, swift1.beginTxn()).getValue(), tmpClk);

    }

    @Test
    public void prune2() {
        tmpClk = generateNew(3);
        registerSingleUpdateTxn(tmpClk, i1, swift1);
        tmpClk2 = generateNew(6);
        CausalityClock c = swift1.beginTxn().getClock();
        registerSingleUpdateTxn(tmpClk2, i1, swift1);

        swift1.prune((CRDT) i1, c);
        // TesterUtils.printInformtion(i1, swift1.beginTxn());
        max.merge(tmpClk);
        max.merge(tmpClk2);
        assertEquals(getTxnLocal(i1, swift1.beginTxn()).getValue(), max);
    }

    @Test(expected = IllegalStateException.class)
    public void prune3() {

        for (int i = 0; i < 5; i++) {
            tmpClk = generateNew(i);
            registerSingleUpdateTxn(tmpClk, i1, swift1);
            max.merge(tmpClk);
        }
        CausalityClock c1 = swift1.beginTxn().getClock();

        for (int i = 0; i < 5; i++) {
            tmpClk = generateNew(i);
            registerSingleUpdateTxn(tmpClk, i1, swift1);
            max.merge(tmpClk);
        }
        CausalityClock c2 = swift1.beginTxn().getClock();

        for (int i = 0; i < 5; i++) {
            tmpClk = generateNew(i);
            registerSingleUpdateTxn(tmpClk, i1, swift1);
            max.merge(tmpClk);
        }

        swift1.prune((CRDT) i1, c2);
        // TesterUtils.printInformtion(i1, swift1.beginTxn());
        assertEquals(getTxnLocal(i1, swift1.beginTxn()).getValue(), max);
        i1.getTxnLocalCopy(c1, swift2.beginTxn());
    }

    @Test
    public void mergePruned1() {

        for (int i = 0; i < 5; i++) {
            tmpClk = generateNew(i);
            registerSingleUpdateTxn(tmpClk, i1, swift1);
            max.merge(tmpClk);
        }
        for (int i = 0; i < 5; i++) {
            tmpClk = generateNew(i);
            registerSingleUpdateTxn(tmpClk, i2, swift2);
            max.merge(tmpClk);
        }
        CausalityClock c2 = swift2.beginTxn().getClock();
        swift2.prune((CRDT) i2, c2);
        swift1.merge((CRDT) i1, (CRDT) i2, swift2);

        // TesterUtils.printInformtion(i1, swift1.beginTxn());
        assertEquals(getTxnLocal(i1, swift1.beginTxn()).getValue(), max);
        //assertTrue(result == 14 || result == 24);
    }

    @Test
    public void mergePruned2() {

        for (int i = 0; i < 5; i++) {
            tmpClk = generateNew(i);
            registerSingleUpdateTxn(tmpClk, i1, swift1);
            max.merge(tmpClk);
        }

        CausalityClock c1 = swift1.beginTxn().getClock();
        swift1.prune((CRDT) i1, c1);

        for (int i = 0; i < 5; i++) {
            tmpClk = generateNew(i);
            registerSingleUpdateTxn(tmpClk, i2, swift2);
            max.merge(tmpClk);
        }
        swift1.merge((CRDT) i1, (CRDT) i2, swift2);

        //TesterUtils.printInformtion(i1, swift1.beginTxn());
        assertEquals(getTxnLocal(i1, swift1.beginTxn()).getValue(), max);
        /*int result = getTxnLocal(i1, swift1.beginTxn()).getValue().i;
         assertTrue(result == 14 || result == 24);*/
    }

    @Test
    public void mergePruned3() {

        for (int i = 0; i < 2; i++) {
            tmpClk = generateNew(i);
            registerSingleUpdateTxn(tmpClk, i1, swift1);
            max.merge(tmpClk);
        }
        CausalityClock c1 = swift1.beginTxn().getClock();
        for (int i = 2; i < 4; i++) {
            tmpClk = generateNew(i);
            registerSingleUpdateTxn(tmpClk, i1, swift1);
            max.merge(tmpClk);
        }
        swift1.prune((CRDT) i1, c1);

        for (int i = 0; i < 2; i++) {
            tmpClk = generateNew(i);
            registerSingleUpdateTxn(tmpClk, i2, swift2);
            max.merge(tmpClk);
        }

        CausalityClock c2 = swift2.beginTxn().getClock();
        for (int i = 2; i < 4; i++) {
            tmpClk = generateNew(i);
            registerSingleUpdateTxn(tmpClk, i2, swift2);
            max.merge(tmpClk);
        }
        swift2.prune((CRDT) i2, c2);
        swift1.merge((CRDT) i1, (CRDT) i2, swift2);

        //TesterUtils.printInformtion(i1, swift1.beginTxn());

        assertEquals(getTxnLocal(i1, swift1.beginTxn()).getValue(), max);
        /*int result = getTxnLocal(i1, swift1.beginTxn()).getValue().i;
         assertTrue(result == 13 || result == 23);*/
    }

    @Test
    public void testGetUpdateTimestampsSince() {
        tmpClk = generateNew(3);

        final CausalityClock updatesSince = i1.getClock().clone();
        assertTrue(i1.getUpdateTimestampsSince(updatesSince).isEmpty());

        registerSingleUpdateTxn(tmpClk, i1, swift1);
        assertEquals(1, i1.getUpdateTimestampsSince(updatesSince).size());
    }
}
