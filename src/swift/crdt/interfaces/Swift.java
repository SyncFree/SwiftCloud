package swift.crdt.interfaces;

/**
 * API for the Swift system.
 * 
 * @author annettebieniusa
 * 
 */
public interface Swift {
	/**
	 * Starts a new transactions.
	 * 
	 * @param cp
	 *            cache policy to be used for the new transaction
	 * @param read_only
	 *            must be set to true if new transaction is read-only
	 * @return TxnHandle for the new transaction
	 */
	TxnHandle beginTxn(CachePolicy cp, boolean read_only);
}
