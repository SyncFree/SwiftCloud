/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
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

	public int hashCode() {
		return key.hashCode();
	}
	
	public boolean equals( Object other ) {
		return other != null && key.equals( ((StringKey)other).key);
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
