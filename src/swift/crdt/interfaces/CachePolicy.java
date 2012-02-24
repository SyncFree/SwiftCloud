package swift.crdt.interfaces;

public enum CachePolicy {
	/**
	 * Strict mode will fail if server cannot be contacted
	 */
	STRICTLY_MOST_RECENT, MOST_RECENT, CACHED;
}
