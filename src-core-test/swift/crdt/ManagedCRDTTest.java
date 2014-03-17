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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.Test;

import swift.client.CommitListener;
import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.clocks.ClockFactory;
import swift.clocks.IncrementalTimestampGenerator;
import swift.clocks.IncrementalTripleTimestampGenerator;
import swift.clocks.Timestamp;
import swift.clocks.TimestampMapping;
import swift.clocks.TripleTimestamp;
import swift.crdt.core.BulkGetProgressListener;
import swift.crdt.core.CRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import swift.crdt.core.CRDTOperationDependencyPolicy;
import swift.crdt.core.CRDTUpdate;
import swift.crdt.core.ManagedCRDT;
import swift.crdt.core.ObjectUpdatesListener;
import swift.crdt.core.TxnHandle;
import swift.crdt.core.TxnStatus;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

public class ManagedCRDTTest {
    private static final CRDTIdentifier ID = new CRDTIdentifier("my", "integer");

    private TestedManagedCRDT<AddWinsSetCRDT<Integer>> a;
    private TestedManagedCRDT<AddWinsSetCRDT<Integer>> b;
    private CausalityClock initClock;

    private static class TestedManagedCRDT<V extends CRDT<V>> extends ManagedCRDT<V> {
        public TestedManagedCRDT(final CRDTIdentifier id, V checkpoint, CausalityClock clock, boolean registered) {
            super(id, checkpoint, clock, registered);
        }

        private List<CRDTObjectUpdatesGroup<V>> getInternalLog() {
            return this.strippedLog;
        }
    }

    @Before
    public void setUp() {
        initClock = ClockFactory.newClock();
        initClock.record(new IncrementalTimestampGenerator("I").generateNew());
        a = new TestedManagedCRDT<AddWinsSetCRDT<Integer>>(ID, new AddWinsSetCRDT<Integer>(ID), initClock.clone(), true);
        b = new TestedManagedCRDT<AddWinsSetCRDT<Integer>>(ID, new AddWinsSetCRDT<Integer>(ID), initClock.clone(), true);
    }

    @Test
    public void testInit() {
        assertEquals(CMP_CLOCK.CMP_EQUALS, initClock.compareTo(a.getClock()));
        assertEquals(CMP_CLOCK.CMP_EQUALS, ClockFactory.newClock().compareTo(a.getPruneClock()));
        assertEquals(ID, a.getUID());
        assertTrue(a.getUpdatesTimestampMappingsSince(ClockFactory.newClock()).isEmpty());
        assertTrue(a.isRegisteredInStore());
    }

    private CRDTObjectUpdatesGroup<AddWinsSetCRDT<Integer>> createUpdatesGroup(String site,
            final CausalityClock dependency, Integer... elements) {
        final TimestampMapping mapping = new TimestampMapping(new IncrementalTimestampGenerator(site).generateNew());
        IncrementalTripleTimestampGenerator generator = new IncrementalTripleTimestampGenerator(
                mapping.getClientTimestamp());
        CRDTObjectUpdatesGroup<AddWinsSetCRDT<Integer>> group = new CRDTObjectUpdatesGroup<AddWinsSetCRDT<Integer>>(ID,
                mapping, null, dependency);
        for (int i = 0; i < elements.length; i++) {
            final Set<TripleTimestamp> emptySet = Collections.emptySet();
            group.append(new SetAddUpdate<Integer, AddWinsSetCRDT<Integer>>(elements[i], generator.generateNew(),
                    emptySet));
        }
        return group;
    }

    @Test
    public void testExecuteUpdateFresh() {
        final CRDTObjectUpdatesGroup<AddWinsSetCRDT<Integer>> group = createUpdatesGroup("X", ClockFactory.newClock(),
                1);
        assertTrue(a.execute(group, CRDTOperationDependencyPolicy.CHECK));

        assertTrue(a.getClock().includes(group.getClientTimestamp()));
        // assertNotSame(a.getInternalLog().get(index)group.getTimestampMapping());
        // assertSame(a.clientTimestampsInUse.get(group.getClientTimestamp()).get(0).getMapping(),
        // a.idsToValues.keySet()
        // .iterator().next().getMapping());
        assertEquals(1, a.getInternalLog().size());
    }

    @Test
    public void testExecuteUpdateIdempotence() {
        final CRDTObjectUpdatesGroup<AddWinsSetCRDT<Integer>> group = createUpdatesGroup("X", ClockFactory.newClock(),
                1);
        assertTrue(a.execute(group, CRDTOperationDependencyPolicy.CHECK));
        assertFalse(a.execute(group, CRDTOperationDependencyPolicy.CHECK));

        assertTrue(a.getClock().includes(group.getClientTimestamp()));
        // assertNotSame(group.getTimestampMapping(),
        // a.clientTimestampsInUse.get(group.getClientTimestamp()).get(0)
        // .getMapping());
        // assertSame(a.clientTimestampsInUse.get(group.getClientTimestamp()).get(0).getMapping(),
        // a.idsToValues.keySet()
        // .iterator().next().getMapping());
        assertEquals(1, a.getInternalLog().size());
    }

    @Test
    public void testExecuteUpdateMappingsOnly() {
        final CRDTObjectUpdatesGroup<AddWinsSetCRDT<Integer>> group = createUpdatesGroup("X", ClockFactory.newClock(),
                1);
        assertTrue(a.execute(group, CRDTOperationDependencyPolicy.CHECK));
        // final TimestampMapping mappingBeforeReexecute =
        // a.clientTimestampsInUse.get(group.getClientTimestamp())
        // .iterator().next().getMapping();

        final CRDTObjectUpdatesGroup<AddWinsSetCRDT<Integer>> commitedGroup = createUpdatesGroup("X",
                ClockFactory.newClock(), 1);
        final Timestamp systemTimestamp = new IncrementalTimestampGenerator("system").generateNew();
        commitedGroup.addSystemTimestamp(systemTimestamp);
        assertFalse(a.execute(commitedGroup, CRDTOperationDependencyPolicy.CHECK));

        assertTrue(a.getClock().includes(group.getClientTimestamp()));
        assertTrue(a.getClock().includes(systemTimestamp));
        // assertNotSame(group.getTimestampMapping(),
        // a.clientTimestampsInUse.get(group.getClientTimestamp()).get(0)
        // .getMapping());
        // assertSame(a.clientTimestampsInUse.get(group.getClientTimestamp()).get(0).getMapping(),
        // a.idsToValues.keySet()
        // .iterator().next().getMapping());
        assertEquals(1, a.getInternalLog().size());
    }

    @Test
    public void testExecuteUpdateUnsatisfiedDependency() {
        final CausalityClock deps = ClockFactory.newClock();
        deps.record(new IncrementalTimestampGenerator("missing").generateNew());
        final CRDTObjectUpdatesGroup<AddWinsSetCRDT<Integer>> group = createUpdatesGroup("X", deps, 1);

        try {
            a.execute(group, CRDTOperationDependencyPolicy.CHECK);
            fail("expected failed dependencies");
        } catch (IllegalStateException x) {
            // expected
        }
        assertFalse(a.getClock().includes(group.getClientTimestamp()));
    }

    @Test
    public void testExecuteUpdateUnsatisfiedDependencyIgnored() {
        final CausalityClock deps = ClockFactory.newClock();
        deps.record(new IncrementalTimestampGenerator("missing").generateNew());
        final CRDTObjectUpdatesGroup<AddWinsSetCRDT<Integer>> group = createUpdatesGroup("X", deps, 1);

        a.execute(group, CRDTOperationDependencyPolicy.IGNORE);
        assertTrue(a.getClock().includes(group.getClientTimestamp()));
    }

    @Test
    public void testExecuteUpdateUnsatisfiedDependencyBlindlyRegistered() {
        final CausalityClock deps = ClockFactory.newClock();
        deps.record(new IncrementalTimestampGenerator("missing").generateNew());
        final CRDTObjectUpdatesGroup<AddWinsSetCRDT<Integer>> group = createUpdatesGroup("X", deps, 1);

        a.execute(group, CRDTOperationDependencyPolicy.RECORD_BLINDLY);
        assertTrue(a.getClock().includes(group.getClientTimestamp()));
        assertEquals(CMP_CLOCK.CMP_DOMINATES, a.getClock().compareTo(deps));
    }

    @Test
    public void testPruneEmpty() {
        a.prune(a.getClock().clone(), true);

        assertEquals(a.getClock(), a.getPruneClock());
        assertTrue(a.getInternalLog().isEmpty());
        try {
            a.getVersion(ClockFactory.newClock(), null);
            fail("Expected rejection of too early clock");
        } catch (IllegalStateException x) {
            // expected
        }
        assertEquals(Collections.EMPTY_SET, a.getLatestVersion(null).getValue());
    }

    @Test
    public void testPruneEverything() {
        final CRDTObjectUpdatesGroup<AddWinsSetCRDT<Integer>> group = createUpdatesGroup("X", ClockFactory.newClock(),
                1);
        a.execute(group, CRDTOperationDependencyPolicy.CHECK);
        a.prune(a.getClock().clone(), true);

        assertEquals(a.getClock(), a.getPruneClock());
        assertTrue(a.getInternalLog().isEmpty());
        try {
            a.getVersion(ClockFactory.newClock(), null);
            fail("Expected rejection of too early clock");
        } catch (IllegalStateException x) {
            // expected
        }
        assertEquals(Collections.singleton(1), a.getLatestVersion(null).getValue());
    }

    @Test
    public void testPrunePartial() {
        final CRDTObjectUpdatesGroup<AddWinsSetCRDT<Integer>> groupX12 = createUpdatesGroup("X",
                ClockFactory.newClock(), 1, 2);
        final CRDTObjectUpdatesGroup<AddWinsSetCRDT<Integer>> groupY3 = createUpdatesGroup("Y",
                ClockFactory.newClock(), 3);

        a.execute(groupX12, CRDTOperationDependencyPolicy.CHECK);
        a.execute(groupY3, CRDTOperationDependencyPolicy.CHECK);
        CausalityClock groupY3Clock = ClockFactory.newClock();
        groupY3Clock.record(groupY3.getClientTimestamp());
        a.prune(groupY3Clock, false);

        assertEquals(1, a.getInternalLog().size());
        assertEquals(CMP_CLOCK.CMP_EQUALS, groupY3Clock.compareTo(a.getPruneClock()));

        try {
            a.getVersion(ClockFactory.newClock(), null);
            fail("Expected rejection of too early clock");
        } catch (IllegalStateException x) {
            // expected
        }
        assertEquals(new HashSet<Integer>(Arrays.asList(3)), a.getVersion(groupY3Clock, null).getValue());
        assertEquals(new HashSet<Integer>(Arrays.asList(1, 2, 3)), a.getLatestVersion(null).getValue());
    }

    @Test
    public void testPruneTwice() {
        a.execute(createUpdatesGroup("X", ClockFactory.newClock(), 1), CRDTOperationDependencyPolicy.CHECK);
        CausalityClock c1 = a.getClock().clone();

        a.execute(createUpdatesGroup("Y", ClockFactory.newClock(), 2), CRDTOperationDependencyPolicy.CHECK);
        CausalityClock c2 = a.getClock().clone();

        a.prune(c1, true);
        a.prune(c2, true);

        a.execute(createUpdatesGroup("Z", ClockFactory.newClock(), 1, 2, 3), CRDTOperationDependencyPolicy.CHECK);

        assertEquals(c2, a.getPruneClock());
        assertEquals(1, a.getInternalLog().size());
        assertEquals(new HashSet<Integer>(Arrays.asList(1, 2, 3)), a.getLatestVersion(null).getValue());
    }

    @Test
    public void testPruneWrongClockCheck() {
        a.execute(createUpdatesGroup("X", ClockFactory.newClock(), 1), CRDTOperationDependencyPolicy.CHECK);
        final CausalityClock dominatingClock = a.getClock().clone();
        dominatingClock.record(new Timestamp("Y", 1));
        final CausalityClock concurrentClock = ClockFactory.newClock();
        concurrentClock.record(new Timestamp("Z", 1));

        try {
            a.prune(dominatingClock, true);
            fail();
        } catch (IllegalStateException x) {
            // expected
        }
        try {
            a.prune(concurrentClock, true);
            fail();
        } catch (IllegalStateException x) {
            // expected
        }

        a.prune(concurrentClock, false);
    }

    @Test
    public void testMergeConcurrent() {
        final CRDTObjectUpdatesGroup<AddWinsSetCRDT<Integer>> groupX1 = createUpdatesGroup("X",
                ClockFactory.newClock(), 1);
        final CRDTObjectUpdatesGroup<AddWinsSetCRDT<Integer>> groupY2 = createUpdatesGroup("Y",
                ClockFactory.newClock(), 2);

        a.execute(groupX1, CRDTOperationDependencyPolicy.CHECK);
        b.execute(groupY2, CRDTOperationDependencyPolicy.CHECK);
        ManagedCRDT<AddWinsSetCRDT<Integer>> aCopy = a.copy();
        a.merge(b);

        assertEquals(2, a.getInternalLog().size());
        assertEquals(CMP_CLOCK.CMP_EQUALS, ClockFactory.newClock().compareTo(a.getPruneClock()));
        assertTrue(a.getClock().includes(groupX1.getClientTimestamp()));
        assertEquals(new HashSet<Integer>(Arrays.asList(1, 2)), a.getLatestVersion(null).getValue());

        b.merge(aCopy);
        assertEquals(2, b.getInternalLog().size());
        assertEquals(CMP_CLOCK.CMP_EQUALS, ClockFactory.newClock().compareTo(b.getPruneClock()));
        assertTrue(b.getClock().includes(groupX1.getClientTimestamp()));
        assertEquals(new HashSet<Integer>(Arrays.asList(1, 2)), b.getLatestVersion(null).getValue());
    }

    @Test
    public void testMergeDominated() {
        final CRDTObjectUpdatesGroup<AddWinsSetCRDT<Integer>> groupX1 = createUpdatesGroup("X",
                ClockFactory.newClock(), 1);
        final CRDTObjectUpdatesGroup<AddWinsSetCRDT<Integer>> groupY2 = createUpdatesGroup("Y",
                ClockFactory.newClock(), 2);

        a.execute(groupX1, CRDTOperationDependencyPolicy.CHECK);
        a.execute(groupY2, CRDTOperationDependencyPolicy.CHECK);
        b.execute(groupX1, CRDTOperationDependencyPolicy.CHECK);
        ManagedCRDT<AddWinsSetCRDT<Integer>> aCopy = a.copy();
        a.merge(b);

        assertEquals(2, a.getInternalLog().size());
        assertEquals(CMP_CLOCK.CMP_EQUALS, ClockFactory.newClock().compareTo(a.getPruneClock()));
        assertTrue(a.getClock().includes(groupX1.getClientTimestamp()));
        assertEquals(new HashSet<Integer>(Arrays.asList(1, 2)), a.getLatestVersion(null).getValue());

        b.merge(aCopy);
        assertEquals(2, b.getInternalLog().size());
        assertEquals(CMP_CLOCK.CMP_EQUALS, ClockFactory.newClock().compareTo(b.getPruneClock()));
        assertTrue(b.getClock().includes(groupX1.getClientTimestamp()));
        assertEquals(new HashSet<Integer>(Arrays.asList(1, 2)), b.getLatestVersion(null).getValue());
    }

    @Test
    public void testMergeIdempotence() {
        final CRDTObjectUpdatesGroup<AddWinsSetCRDT<Integer>> groupX1 = createUpdatesGroup("X",
                ClockFactory.newClock(), 1);

        a.execute(groupX1, CRDTOperationDependencyPolicy.CHECK);
        final ManagedCRDT<AddWinsSetCRDT<Integer>> aCopy = a.copy();
        a.merge(aCopy);
        a.merge(aCopy);

        assertEquals(1, a.getInternalLog().size());
        assertEquals(CMP_CLOCK.CMP_EQUALS, ClockFactory.newClock().compareTo(a.getPruneClock()));
        assertTrue(a.getClock().includes(groupX1.getClientTimestamp()));
        assertEquals(new HashSet<Integer>(Arrays.asList(1)), a.getLatestVersion(null).getValue());
    }

    @Test
    public void testMergePrunedConcurrent() {
        a.execute(createUpdatesGroup("X", ClockFactory.newClock(), 1), CRDTOperationDependencyPolicy.CHECK);

        b.execute(createUpdatesGroup("Y", ClockFactory.newClock(), 2), CRDTOperationDependencyPolicy.CHECK);
        b.prune(b.getClock().clone(), true);
        a.merge(b);

        assertEquals(new HashSet<Integer>(Arrays.asList(1, 2)), a.getLatestVersion(null).getValue());
    }

    @Test
    public void testMergeComplexWithPruningAndExtraMappings() {
        final CRDTObjectUpdatesGroup<AddWinsSetCRDT<Integer>> prunedUpdates1 = createUpdatesGroup("X",
                ClockFactory.newClock(), 1);
        a.execute(prunedUpdates1, CRDTOperationDependencyPolicy.CHECK);
        // Note: b has more mappings for prunedUpdates.
        prunedUpdates1.addSystemTimestamp(new Timestamp("DC1", 1));
        b.execute(prunedUpdates1, CRDTOperationDependencyPolicy.CHECK);
        CausalityClock c1 = b.getClock().clone();

        final CRDTObjectUpdatesGroup<AddWinsSetCRDT<Integer>> nonprunedUpdates2 = createUpdatesGroup("Y",
                ClockFactory.newClock(), 2);
        b.execute(nonprunedUpdates2, CRDTOperationDependencyPolicy.CHECK);
        // Note: a has more mappings for nonprunedUpdates
        nonprunedUpdates2.addSystemTimestamp(new Timestamp("DC2", 1));
        a.execute(nonprunedUpdates2, CRDTOperationDependencyPolicy.CHECK);

        final CRDTObjectUpdatesGroup<AddWinsSetCRDT<Integer>> aPrivateUpdates3 = createUpdatesGroup("Z",
                ClockFactory.newClock(), 3);
        a.execute(aPrivateUpdates3, CRDTOperationDependencyPolicy.CHECK);

        b.prune(c1, true);

        a.merge(b);
        assertEquals(new HashSet<Integer>(Arrays.asList(1, 2, 3)), a.getLatestVersion(null).getValue());
        assertEquals(2, a.getInternalLog().size());

        final CausalityClock referenceViaMergedMapping = c1.clone();
        referenceViaMergedMapping.record(new Timestamp("DC1", 1));
        assertEquals(new HashSet<Integer>(Arrays.asList(1)), a.getVersion(referenceViaMergedMapping, null).getValue());

        final CausalityClock referenceViaOriginalMapping = c1.clone();
        referenceViaOriginalMapping.record(new Timestamp("DC2", 1));
        assertEquals(new HashSet<Integer>(Arrays.asList(1, 2)), a.getVersion(referenceViaOriginalMapping, null)
                .getValue());
    }

    @Test
    public void testTimestampMappingsSince() {
        final CausalityClock updatesSince = a.getClock().clone();
        assertTrue(a.getUpdatesTimestampMappingsSince(updatesSince).isEmpty());
        assertTrue(a.getUpdatesTimestampMappingsSince(ClockFactory.newClock()).isEmpty());

        a.execute(createUpdatesGroup("X", ClockFactory.newClock(), 1), CRDTOperationDependencyPolicy.CHECK);
        a.execute(createUpdatesGroup("Y", ClockFactory.newClock(), 2), CRDTOperationDependencyPolicy.CHECK);
        assertEquals(2, a.getUpdatesTimestampMappingsSince(updatesSince).size());
        assertTrue(a.getUpdatesTimestampMappingsSince(a.getClock().clone()).isEmpty());
    }

    @Test
    public void testTimestampMappingsSincePruned() {
        final CausalityClock updatesSince = a.getClock().clone();
        a.execute(createUpdatesGroup("X", ClockFactory.newClock(), 1), CRDTOperationDependencyPolicy.CHECK);
        a.prune(a.getClock().clone(), true);
        try {
            a.getUpdatesTimestampMappingsSince(updatesSince);
            fail();
        } catch (IllegalArgumentException x) {
            // expected
        }
    }

    @Test
    public void testUnregisteredObject() {
        final ManagedCRDT<AddWinsSetCRDT<Integer>> unregisteredObject = new ManagedCRDT<AddWinsSetCRDT<Integer>>(ID,
                new AddWinsSetCRDT<Integer>(ID), ClockFactory.newClock(), false);
        assertFalse(unregisteredObject.isRegisteredInStore());
        final AtomicReference<AddWinsSetCRDT<Integer>> creationStateRef = new AtomicReference<AddWinsSetCRDT<Integer>>();
        unregisteredObject.getLatestVersion(new TxnHandle() {

            @Override
            public void rollback() {
                fail();
            }

            @Override
            public <V extends CRDT<V>> void registerOperation(CRDTIdentifier id, CRDTUpdate<V> op) {
                fail();
            }

            @Override
            public <V extends CRDT<V>> void registerObjectCreation(CRDTIdentifier id, V creationState) {
                creationStateRef.set((AddWinsSetCRDT<Integer>) creationState);
                assertEquals(ID, id);
            }

            @Override
            public TripleTimestamp nextTimestamp() {
                fail();
                return null;
            }

            @Override
            public TxnStatus getStatus() {
                fail();
                return null;
            }

            @Override
            public <V extends CRDT<V>> V get(CRDTIdentifier id, boolean create, Class<V> classOfV,
                    ObjectUpdatesListener updatesListener) throws WrongTypeException, NoSuchObjectException,
                    VersionNotFoundException, NetworkException {
                fail();
                return null;
            }

            @Override
            public <V extends CRDT<V>> V get(CRDTIdentifier id, boolean create, Class<V> classOfV)
                    throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
                fail();
                return null;
            }

            @Override
            public void commitAsync(CommitListener listener) {
                fail();
            }

            @Override
            public void commit() {
                fail();
            }

            @Override
            public Map<CRDTIdentifier, CRDT<?>> bulkGet(CRDTIdentifier... ids) {
                fail();
                return null;
            }

            @Override
            public Map<CRDTIdentifier, CRDT<?>> bulkGet(Set<CRDTIdentifier> ids, BulkGetProgressListener listener) {
                fail();
                return null;
            }
        });
        assertNotNull(creationStateRef.get());

        unregisteredObject.execute(new CRDTObjectUpdatesGroup<AddWinsSetCRDT<Integer>>(ID, new TimestampMapping(
                new Timestamp("site", 1)), creationStateRef.get(), ClockFactory.newClock()),
                CRDTOperationDependencyPolicy.CHECK);
        assertTrue(unregisteredObject.isRegisteredInStore());
    }
}
