package swift.test.crdt;

import java.util.HashMap;
import java.util.Map;

import swift.clocks.CCIncrementalTimestampGenerator;
import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTOperation;
import swift.crdt.interfaces.TxnHandle;

public class TxnHandleForTesting implements TxnHandle {
    private Map<CRDTIdentifier, CRDT<?, ?>> cache;
    private CausalityClock cc;
    private Timestamp ts;
    private String siteId;
    private int updateCounter;

    public TxnHandleForTesting(String siteId, CausalityClock cc) {
        this.cache = new HashMap<CRDTIdentifier, CRDT<?, ?>>();
        this.cc = cc;
        this.siteId = siteId;
        this.ts = new CCIncrementalTimestampGenerator(this.siteId).generateNew(this.cc);
        this.updateCounter = 0;
    }

    @Override
    public <V extends CRDT<V, I>, I> V get(CRDTIdentifier id, boolean create, Class<V> classOfT) {

        if (create) {
            try {
                V obj = classOfT.newInstance();
                obj.setTxnHandle(this);
                obj.setUID(id);
                obj.setClock(cc); // FIXME is this correct?
                this.cache.put(id, obj);
                return obj;
            } catch (InstantiationException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } else {
            throw new RuntimeException("Not implemented yet!");
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
        return new TripleTimestamp(ts.getIdentifier(), ts.getCounter(), updateCounter++);
    }

    @Override
    public CausalityClock getClock() {
        return this.cc;
    }

    @Override
    public <I extends CRDTOperation> void registerOperation(I op) {
        CRDT<?, I> target = (CRDT<?, I>) this.cache.get(op.getTargetUID());
        target.execute(op);
    }

}
