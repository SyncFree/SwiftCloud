package swift.test.crdt;

import java.util.HashMap;
import java.util.Map;

import swift.clocks.CausalityClock;
import swift.clocks.IncrementalTimestampGenerator;
import swift.clocks.IncrementalTripleTimestampGenerator;
import swift.clocks.TimestampSource;
import swift.clocks.TripleTimestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTOperation;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;
import swift.crdt.interfaces.TxnStatus;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.WrongTypeException;

public class TxnHandleForTesting implements TxnHandle {
    private Map<CRDTIdentifier, TxnLocalCRDT<?>> cache;
    private CausalityClock cc;
    private TimestampSource<TripleTimestamp> timestampGenerator;

    public TxnHandleForTesting(String siteId, CausalityClock cc) {
        this.cache = new HashMap<CRDTIdentifier, TxnLocalCRDT<?>>();
        this.cc = cc;
        this.timestampGenerator = new IncrementalTripleTimestampGenerator(
                new IncrementalTimestampGenerator(siteId, 0).generateNew());
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V extends CRDT<V>, T extends TxnLocalCRDT<V>> T get(CRDTIdentifier id, boolean create, Class<V> classOfV)
            throws WrongTypeException, NoSuchObjectException {

        if (create) {
            try {
                V crdt = classOfV.newInstance();
                crdt.setUID(id);
                crdt.setClock(cc);
                TxnLocalCRDT<V> localView = crdt.getTxnLocalCopy(getSnapshotClock(), getSnapshotClock(), this);
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
        throw new RuntimeException("Not supported for testing!");
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
    public CausalityClock getSnapshotClock() {
        return this.cc;
    }

    @Override
    public void registerOperation(CRDTIdentifier id, CRDTOperation op) {
        // NOP
    }

    @Override
    public TxnStatus getStatus() {
        return null;
    }

}
