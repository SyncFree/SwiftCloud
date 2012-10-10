package swift.crdt;

import swift.clocks.CausalityClock;
import swift.crdt.interfaces.CRDTUpdate;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;

public class SortedSetVersioned<V extends Comparable<V>> extends AbstractSortedSetVersioned<V, SortedSetVersioned<V>> {
	private static final long serialVersionUID = 1L;

	public SortedSetVersioned() {
	}

	@Override
	protected TxnLocalCRDT<SortedSetVersioned<V>> getTxnLocalCopyImpl(CausalityClock versionClock, TxnHandle txn) {
		final SortedSetVersioned<V> creationState = isRegisteredInStore() ? null : new SortedSetVersioned<V>();
		SortedSetTxnLocal<V> localView = new SortedSetTxnLocal<V>(id, txn, versionClock, creationState, getValue(versionClock));
		return localView;
	}

	@Override
	protected void execute(CRDTUpdate<SortedSetVersioned<V>> op) {
		op.applyTo(this);
	}

	@Override
	public SortedSetVersioned<V> copy() {
		SortedSetVersioned<V> copy = new SortedSetVersioned<V>();
		copyLoad(copy);
		copyBase(copy);
		return copy;
	}
}
	