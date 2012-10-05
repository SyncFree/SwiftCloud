package swift.crdt;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.clocks.ClockFactory;
import swift.clocks.IncrementalTimestampGenerator;
import swift.clocks.IncrementalTripleTimestampGenerator;
import swift.clocks.Timestamp;
import swift.clocks.TimestampMapping;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDTOperationDependencyPolicy;
import swift.crdt.interfaces.CRDTUpdate;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;
import swift.crdt.operations.BaseUpdate;
import swift.crdt.operations.CRDTObjectUpdatesGroup;

public class BaseCRDTTest {
    private static final CRDTIdentifier ID = new CRDTIdentifier("my", "integer");

    // A very simple, but representative CRDT that is both prunable, may need
    // timestamps merging etc.
    static class MaxIntegerCRDT extends BaseCRDT<MaxIntegerCRDT> {
        Map<TripleTimestamp, Integer> idsToValues = new HashMap<TripleTimestamp, Integer>();

        @Override
        protected void pruneImpl(CausalityClock pruningPoint) {
            Set<TripleTimestamp> toRemove = new HashSet<TripleTimestamp>();
            Integer maxValue = null;
            TripleTimestamp maxValueTs = null;
            for (final Entry<TripleTimestamp, Integer> entry : idsToValues.entrySet()) {
                if (entry.getKey().timestampsIntersect(pruningPoint)) {
                    toRemove.add(entry.getKey());
                }
                if (maxValue == null || entry.getValue() > maxValue) {
                    maxValue = entry.getValue();
                    maxValueTs = entry.getKey();
                }
            }

            if (maxValue != null) {
                // Leave the representative
                toRemove.remove(maxValueTs);
            }

            for (final TripleTimestamp ts : toRemove) {
                unregisterTimestampUsage(ts);
                idsToValues.remove(ts);
            }
        }

        @Override
        protected void mergePayload(MaxIntegerCRDT otherObject) {
            for (Entry<TripleTimestamp, Integer> otherEntry : otherObject.idsToValues.entrySet()) {
                if (!idsToValues.containsKey(otherEntry.getKey())) {
                    idsToValues.put(otherEntry.getKey(), otherEntry.getValue());
                    registerTimestampUsage(otherEntry.getKey());
                }
            }
        }

        @Override
        protected void execute(CRDTUpdate<MaxIntegerCRDT> op) {
            op.applyTo(this);
        }

        @Override
        protected TxnLocalCRDT<MaxIntegerCRDT> getTxnLocalCopyImpl(CausalityClock versionClock, TxnHandle txn) {
            return null;
        }

        public void applyAssign(TripleTimestamp timestamp, Integer value) {
            assertNull(idsToValues.put(timestamp, value));
            registerTimestampUsage(timestamp);
        }
    }

    static class MaxIntegerUpdate extends BaseUpdate<MaxIntegerCRDT> {
        Integer value;

        public MaxIntegerUpdate(TripleTimestamp ts, Integer value) {
            super(ts);
            this.value = value;
        }

        @Override
        public void applyTo(MaxIntegerCRDT crdt) {
            crdt.applyAssign(getTimestamp(), value);
        }
    }

    private MaxIntegerCRDT a;
    private MaxIntegerCRDT b;
    private CausalityClock initClock;

    @Before
    public void setUp() {
        a = new MaxIntegerCRDT();
        initClock = ClockFactory.newClock();
        initClock.record(new IncrementalTimestampGenerator("I").generateNew());
        a.init(ID, initClock.clone(), ClockFactory.newClock(), false);

        b = new MaxIntegerCRDT();
        b.init(new CRDTIdentifier("b", "b"), initClock.clone(), ClockFactory.newClock(), false);
    }

    @Test
    public void testInit() {
        assertEquals(CMP_CLOCK.CMP_EQUALS, initClock.compareTo(a.getClock()));
        assertEquals(CMP_CLOCK.CMP_EQUALS, ClockFactory.newClock().compareTo(a.getPruneClock()));
        assertEquals(ID, a.getUID());
        assertTrue(a.getUpdatesTimestampMappingsSince(ClockFactory.newClock()).isEmpty());
        assertFalse(a.isRegisteredInStore());
    }

    private CRDTObjectUpdatesGroup<MaxIntegerCRDT> createUpdatesGroup(String site, final int numberOfUpdates,
            final CausalityClock dependency) {
        final TimestampMapping mapping = new TimestampMapping(new IncrementalTimestampGenerator(site).generateNew());
        IncrementalTripleTimestampGenerator generator = new IncrementalTripleTimestampGenerator(mapping);
        CRDTObjectUpdatesGroup<MaxIntegerCRDT> group = new CRDTObjectUpdatesGroup<MaxIntegerCRDT>(ID, mapping, null,
                dependency);
        for (int i = 0; i < numberOfUpdates; i++) {
            group.append(new MaxIntegerUpdate(generator.generateNew(), i));
        }
        return group;
    }

    @Test
    public void testExecuteUpdateFresh() {
        final CRDTObjectUpdatesGroup<MaxIntegerCRDT> group = createUpdatesGroup("X", 1, ClockFactory.newClock());
        assertTrue(a.execute(group, CRDTOperationDependencyPolicy.CHECK));

        assertTrue(a.getClock().includes(group.getClientTimestamp()));
        assertNotSame(group.getTimestampMapping(), a.clientTimestampsInUse.get(group.getClientTimestamp()).get(0)
                .getMapping());
        assertSame(a.clientTimestampsInUse.get(group.getClientTimestamp()).get(0).getMapping(), a.idsToValues.keySet()
                .iterator().next().getMapping());
        assertEquals(1, a.idsToValues.size());
    }

    @Test
    public void testExecuteUpdateIdempotence() {
        final CRDTObjectUpdatesGroup<MaxIntegerCRDT> group = createUpdatesGroup("X", 1, ClockFactory.newClock());
        assertTrue(a.execute(group, CRDTOperationDependencyPolicy.CHECK));
        assertFalse(a.execute(group, CRDTOperationDependencyPolicy.CHECK));

        assertTrue(a.getClock().includes(group.getClientTimestamp()));
        assertNotSame(group.getTimestampMapping(), a.clientTimestampsInUse.get(group.getClientTimestamp()).get(0)
                .getMapping());
        assertSame(a.clientTimestampsInUse.get(group.getClientTimestamp()).get(0).getMapping(), a.idsToValues.keySet()
                .iterator().next().getMapping());
        assertEquals(1, a.idsToValues.size());
    }

    @Test
    public void testExecuteUpdateMappingsOnly() {
        final CRDTObjectUpdatesGroup<MaxIntegerCRDT> group = createUpdatesGroup("X", 1, ClockFactory.newClock());
        assertTrue(a.execute(group, CRDTOperationDependencyPolicy.CHECK));
        final TimestampMapping mappingBeforeReexecute = a.clientTimestampsInUse.get(group.getClientTimestamp())
                .iterator().next().getMapping();

        final CRDTObjectUpdatesGroup<MaxIntegerCRDT> commitedGroup = createUpdatesGroup("X", 1, ClockFactory.newClock());
        final Timestamp systemTimestamp = new IncrementalTimestampGenerator("system").generateNew();
        commitedGroup.addSystemTimestamp(systemTimestamp);
        assertFalse(a.execute(commitedGroup, CRDTOperationDependencyPolicy.CHECK));

        assertTrue(a.getClock().includes(group.getClientTimestamp()));
        assertTrue(a.getClock().includes(systemTimestamp));
        assertNotSame(group.getTimestampMapping(), a.clientTimestampsInUse.get(group.getClientTimestamp()).get(0)
                .getMapping());
        assertSame(a.clientTimestampsInUse.get(group.getClientTimestamp()).get(0).getMapping(), a.idsToValues.keySet()
                .iterator().next().getMapping());
        assertEquals(1, a.idsToValues.size());
    }

    @Test
    public void testExecuteUpdateUnsatisfiedDependency() {
        final CausalityClock deps = ClockFactory.newClock();
        deps.record(new IncrementalTimestampGenerator("missing").generateNew());
        final CRDTObjectUpdatesGroup<MaxIntegerCRDT> group = createUpdatesGroup("X", 1, deps);

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
        final CRDTObjectUpdatesGroup<MaxIntegerCRDT> group = createUpdatesGroup("X", 1, deps);

        a.execute(group, CRDTOperationDependencyPolicy.IGNORE);
        assertTrue(a.getClock().includes(group.getClientTimestamp()));
    }

    @Test
    public void testExecuteUpdateUnsatisfiedDependencyBlindlyRegistered() {
        final CausalityClock deps = ClockFactory.newClock();
        deps.record(new IncrementalTimestampGenerator("missing").generateNew());
        final CRDTObjectUpdatesGroup<MaxIntegerCRDT> group = createUpdatesGroup("X", 1, deps);

        a.execute(group, CRDTOperationDependencyPolicy.RECORD_BLINDLY);
        assertTrue(a.getClock().includes(group.getClientTimestamp()));
        assertEquals(CMP_CLOCK.CMP_DOMINATES, a.getClock().compareTo(deps));
    }

    @Test
    public void testPruning() {
        final CRDTObjectUpdatesGroup<MaxIntegerCRDT> group5 = createUpdatesGroup("X", 5, ClockFactory.newClock());
        final CRDTObjectUpdatesGroup<MaxIntegerCRDT> group6 = createUpdatesGroup("Y", 6, ClockFactory.newClock());

        a.execute(group5, CRDTOperationDependencyPolicy.CHECK);
        a.execute(group6, CRDTOperationDependencyPolicy.CHECK);
        CausalityClock group5Clock = ClockFactory.newClock();
        group5Clock.record(group5.getClientTimestamp());
        a.prune(group5Clock, false);

        assertEquals(6, a.idsToValues.size());
        assertEquals(1, a.clientTimestampsInUse.size());
        assertEquals(CMP_CLOCK.CMP_EQUALS, group5Clock.compareTo(a.getPruneClock()));
    }

    @Test
    public void testMerge() {
        final CRDTObjectUpdatesGroup<MaxIntegerCRDT> group5 = createUpdatesGroup("X", 5, ClockFactory.newClock());
        final CRDTObjectUpdatesGroup<MaxIntegerCRDT> group6 = createUpdatesGroup("Y", 6, ClockFactory.newClock());

        a.execute(group5, CRDTOperationDependencyPolicy.CHECK);
        b.execute(group6, CRDTOperationDependencyPolicy.CHECK);
        a.merge(b);

        assertEquals(11, a.idsToValues.size());
        assertEquals(2, a.clientTimestampsInUse.size());
        assertEquals(CMP_CLOCK.CMP_EQUALS, ClockFactory.newClock().compareTo(a.getPruneClock()));
        assertTrue(a.getClock().includes(group5.getClientTimestamp()));
        assertTrue(a.getClock().includes(group6.getClientTimestamp()));
    }

    // TODO merge idemptonce, mappings merge
}
