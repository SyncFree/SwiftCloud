package swift.crdt;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import swift.client.CommitListener;
import swift.clocks.CausalityClock;
import swift.clocks.ClockFactory;
import swift.clocks.IncrementalTimestampGenerator;
import swift.clocks.IncrementalTripleTimestampGenerator;
import swift.clocks.Timestamp;
import swift.clocks.TimestampMapping;
import swift.clocks.TimestampSource;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTOperationDependencyPolicy;
import swift.crdt.interfaces.CRDTUpdate;
import swift.crdt.interfaces.ObjectUpdatesListener;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;
import swift.crdt.interfaces.TxnStatus;
import swift.crdt.operations.CRDTObjectUpdatesGroup;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.WrongTypeException;

public class TxnTester implements TxnHandle {
    // @Annette: this class is simply intended to work with a single object,
    // isn't it? If so, maybe those are really unnecessary.

    // @Marek: to test the recursive data structures, such as dictionary, I need
    // more than one object.
    // Therefore, I had to add the cache.
    // Do you think it is better to provide another TxnTester for this?

    // @Pascal: The TxnTester is not a full txn implementation. In particular,
    // it cannot commit anything to a data center. Hence, the register operation
    // is not needed *on purpose*.

    protected Map<CRDTIdentifier, TxnLocalCRDT<?>> cache;
    protected Map<CRDT<?>, CRDTObjectUpdatesGroup<?>> objectOperations;
    protected CausalityClock cc;
    protected TimestampSource<TripleTimestamp> timestampGenerator;
    protected Timestamp globalTimestamp;
    protected TimestampMapping tm;

    public TxnTester(String siteId, CausalityClock cc) {
        this(siteId, cc, new IncrementalTimestampGenerator(siteId).generateNew(), new IncrementalTimestampGenerator(
                "global:" + siteId).generateNew());
    }

    public TxnTester(String siteId, CausalityClock latestVersion, Timestamp ts, final Timestamp globalTs) {
        this.cache = new HashMap<CRDTIdentifier, TxnLocalCRDT<?>>();
        this.objectOperations = new HashMap<CRDT<?>, CRDTObjectUpdatesGroup<?>>();
        this.cc = latestVersion;
        this.tm = new TimestampMapping(ts);
        this.timestampGenerator = new IncrementalTripleTimestampGenerator(tm);
        this.globalTimestamp = globalTs;
    }

    public <V extends CRDT<V>, T extends TxnLocalCRDT<V>> T get(CRDTIdentifier id, boolean create, Class<V> classOfV)
            throws WrongTypeException, NoSuchObjectException {
        return get(id, create, classOfV, null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V extends CRDT<V>, T extends TxnLocalCRDT<V>> T get(CRDTIdentifier id, boolean create, Class<V> classOfV,
            ObjectUpdatesListener listener) throws WrongTypeException, NoSuchObjectException {

        try {
            TxnLocalCRDT<?> cached = cache.get(id);
            if (cached == null && create) {
                V crdt = classOfV.newInstance();
                crdt.init(id, cc, ClockFactory.newClock(), true);
                TxnLocalCRDT<V> localView = crdt.getTxnLocalCopy(getClock(), this);
                cache.put(id, localView);
                return (T) localView;
            } else {
                return (T) cached;
            }
        } catch (ClassCastException x) {
            throw new WrongTypeException(x.getMessage());
        } catch (InstantiationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return null;

    }

    @Override
    public void commit() {
        commit(false);
    }

    public void commit(boolean globalCommit) {
        for (final Entry<CRDT<?>, CRDTObjectUpdatesGroup<?>> entry : objectOperations.entrySet()) {
            if (globalCommit) {
                entry.getValue().addSystemTimestamp(globalTimestamp);
            }
            entry.getKey().execute((CRDTObjectUpdatesGroup) entry.getValue(), CRDTOperationDependencyPolicy.CHECK);
        }
        for (final Timestamp ts : tm.getTimestamps()) {
            cc.record(ts);
        }
    }

    @Override
    public void commitAsync(final CommitListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rollback() {
        throw new RuntimeException("Not supported for testing!");
    }

    @Override
    public TripleTimestamp nextTimestamp() {
        return timestampGenerator.generateNew();
    }

    public <V extends CRDT<V>> void registerObjectCreation(CRDTIdentifier id, V creationState) {
    }

    @Override
    public TxnStatus getStatus() {
        return null;
    }

    public void updateClock(CausalityClock c) {
        cc.merge(c);
    }

    public CausalityClock getClock() {
        return this.cc.clone();
    }

    // Short-cut for testing purpose
    public <V extends CRDT<V>> void registerOperation(CRDT<V> obj, CRDTUpdate<V> op) {
        final CRDTObjectUpdatesGroup<V> opGroup = new CRDTObjectUpdatesGroup<V>(obj.getUID(), tm, null, cc.clone());
        opGroup.append(op);
        objectOperations.put(obj, opGroup);
    }

    @Override
    public <V extends CRDT<V>> void registerOperation(CRDTIdentifier id, CRDTUpdate<V> op) {
        // NOP: This txn implementation does not apply updates to the underlying
        // CRDT objects
    }

}
