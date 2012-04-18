package swift.test.crdt;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import swift.client.CommitListener;
import swift.clocks.CausalityClock;
import swift.clocks.ClockFactory;
import swift.clocks.IncrementalTimestampGenerator;
import swift.clocks.IncrementalTripleTimestampGenerator;
import swift.clocks.Timestamp;
import swift.clocks.TimestampSource;
import swift.clocks.TripleTimestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTOperation;
import swift.crdt.interfaces.ObjectUpdatesListener;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;
import swift.crdt.interfaces.TxnStatus;
import swift.crdt.operations.CRDTObjectOperationsGroup;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.WrongTypeException;

public class TxnTester implements TxnHandle {
    // @Annette: this class is simply intended to work with a single object,
    // isn't it? If so, maybe those are really unnecessary.
    private Map<CRDTIdentifier, TxnLocalCRDT<?>> cache;
    private Map<CRDT<?>, CRDTObjectOperationsGroup<?>> objectOperations;
    private CausalityClock cc;
    private TimestampSource<TripleTimestamp> timestampGenerator;
    private Timestamp ts;

    public TxnTester(String siteId, CausalityClock cc) {
        this(siteId, cc, new IncrementalTimestampGenerator(siteId, 0).generateNew());
    }

    public TxnTester(String siteId, CausalityClock latestVersion, Timestamp ts) {
        this.cache = new HashMap<CRDTIdentifier, TxnLocalCRDT<?>>();
        this.objectOperations = new HashMap<CRDT<?>, CRDTObjectOperationsGroup<?>>();
        this.cc = latestVersion;
        this.ts = ts;
        this.timestampGenerator = new IncrementalTripleTimestampGenerator(ts);
    }

    public <V extends CRDT<V>, T extends TxnLocalCRDT<V>> T get(CRDTIdentifier id, boolean create, Class<V> classOfV)
            throws WrongTypeException, NoSuchObjectException {
        return get(id, create, classOfV, null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V extends CRDT<V>, T extends TxnLocalCRDT<V>> T get(CRDTIdentifier id, boolean create, Class<V> classOfV,
            ObjectUpdatesListener listener) throws WrongTypeException, NoSuchObjectException {

        if (create) {
            try {
                V crdt = classOfV.newInstance();
                crdt.init(id, cc, ClockFactory.newClock(), true);
                TxnLocalCRDT<V> localView = crdt.getTxnLocalCopy(getClock(), this);
                cache.put(id, localView);
                return (T) localView;
            } catch (ClassCastException x) {
                throw new WrongTypeException(x.getMessage());
            } catch (InstantiationException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        } else {
            throw new RuntimeException("Not implemented in TxnHandleForTesting!");
        }
        return null;

    }

    @Override
    public void commit() {
        for (final Entry<CRDT<?>, CRDTObjectOperationsGroup<?>> entry : objectOperations.entrySet()) {
            entry.getKey().execute((CRDTObjectOperationsGroup) entry.getValue(), false);
        }
        cc.record(ts);
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

    @Override
    public <V extends CRDT<V>> void registerOperation(CRDTIdentifier id, CRDTOperation<V> op) {
        // NOP
    }

    // Short-cut for testing purpose
    public <V extends CRDT<V>> void registerOperation(CRDT<V> obj, CRDTOperation<V> op) {
        final CRDTObjectOperationsGroup<V> opGroup = new CRDTObjectOperationsGroup<V>(obj.getUID(), cc, ts, null);
        opGroup.append(op);
        objectOperations.put(obj, opGroup);
    }

    @Override
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

}
