package sys.dht.api;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * A wrapper for using a String as a DHT key
 * 
 * @author smd
 * 
 */
public class StringKey implements DHT.Key {

	private static MessageDigest digest;

	String key;

	public StringKey() {
	}

	public StringKey(final String key) {
		this.key = key;
	}

	public String value() {
		return key;
	}

	@Override
	public long longHashValue() {
		synchronized (digest) {
			digest.reset();
			digest.update(key.getBytes());
			return new BigInteger(1, digest.digest()).longValue() >>> 1;
		}
	}

	static {
		try {
			digest = java.security.MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}
}
