package swift.crdt;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTUpdate;
import swift.crdt.operations.CRDTObjectUpdatesGroup;

/*
 *  Pseudo Txn that linked to a unique CRDT.
 *  It applies the operations to the CRDT.
 */

public class TxnTesterWithRegister extends TxnTester {
    private CRDT<?> target = null;

    public <V extends CRDT<V>> TxnTesterWithRegister(String siteId, CausalityClock latestVersion, Timestamp ts,
            Timestamp globalTs, V target) {
        super(siteId, latestVersion, ts, globalTs);
        this.target = target;
    }

    public <V extends CRDT<V>> void registerOperation(CRDTIdentifier id, CRDTUpdate<V> op) {
        if (target != null) {
            CRDTObjectUpdatesGroup<V> opGroup = (CRDTObjectUpdatesGroup<V>) objectOperations.get(target);
            if (opGroup == null) {
                opGroup = new CRDTObjectUpdatesGroup<V>(target.getUID(), tm, null, cc);
            }
            opGroup.append(op);
            objectOperations.put(target, opGroup);
        }
    }
}
