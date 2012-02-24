package swift.crdt.interfaces;

public interface Swift {
	TxnHandle beginTxn(CachePolicy cp, boolean read_only);
}
