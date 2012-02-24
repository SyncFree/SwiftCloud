package swift.crdt.interfaces;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;

public interface TxnHandle {

	Timestamp nextTimestamp();

	CausalityClock getClock();

	void registerOperation(CRDTOperation op);

	/**************/

	<T extends CRDT<T>> CRDT<T> get(CRDTIdentifier id, boolean create,
			Class<T> classOfT);

	void commit();

	void rollback();
}
