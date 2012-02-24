package swift.crdt.interfaces;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.utils.Pair;

public interface CRDT<V extends CRDT<V>> {
	void merge(V other);

	void execute(CRDTOperation op);

	Pair<String, String> getUID();

	void setUID();

	CausalityClock getClock();

	void setClock(CausalityClock c);

	TxnHandle getTxnHandle();

	void setTxnHandle(TxnHandle txn);

	void prune(CausalityClock c);

	void rollback(Timestamp ts);
}
