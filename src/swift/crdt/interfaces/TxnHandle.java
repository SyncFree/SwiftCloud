package swift.crdt.interfaces;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;

public interface TxnHandle {

	Timestamp nextTimestamp();

	CausalityClock getClock();

	void registerOperation(CRDTOperation op);

	/**************/

	CRDT<?> get(String table, String key);

	void commit();

	void rollback();
}
