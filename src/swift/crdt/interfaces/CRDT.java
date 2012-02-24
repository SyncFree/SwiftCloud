package swift.crdt.interfaces;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;

public interface CRDT<V extends CRDT<V>> {
	void merge(V other);

	void execute(CRDTOperation op);

	void prune(CausalityClock c);

	void rollback(Timestamp ts);

	// only used by system

	CRDTIdentifier getUID();

	void setUID(CRDTIdentifier id);

	CausalityClock getClock();

	void setClock(CausalityClock c);

	TxnHandle getTxnHandle();

	void setTxnHandle(TxnHandle txn);

}
