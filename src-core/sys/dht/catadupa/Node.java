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
package sys.dht.catadupa;

import static sys.dht.catadupa.Config.Config;

import java.math.BigInteger;
import java.util.Random;

import sys.net.api.Endpoint;

/**
 * 
 * @author smd
 * 
 */
public class Node {

	public long key;
	public Endpoint endpoint;
	public String datacenter;

	public Node() {
	}

	protected Node(Node other) {
		endpoint = other.endpoint;
		datacenter = other.datacenter;
		key = locator2key(endpoint.gid());
	}

	public Node(Endpoint endpoint) {
		this(endpoint, "?");
	}

	public Node(Endpoint endpoint, String datacenter) {
		this.endpoint = endpoint;
		this.datacenter = datacenter;
		key = locator2key(endpoint.gid());
	}

	@Override
	public int hashCode() {
		return (int)((key >>> 32) ^ key);
	}
	
	public boolean equals( Object other ) {
		return other != null && ((Node)other).key == key;
	}
	
	public String getDatacenter() {
		return datacenter;
	}

	public boolean isOnline() {
		return true;
	}

	public boolean isOffline() {
		return !isOnline();
	}

	@Override
	public String toString() {
		return "" + key ; //+ " -> " + endpoint ;
	}

	private static long locator2key(Object locator) {
		return new BigInteger(Config.NODE_KEY_LENGTH, new Random((Long) locator)).longValue();
	}
}

class DeadNode extends Node {

	public DeadNode(Node other) {
		key = other.key;
		endpoint = other.endpoint;
		datacenter = other.datacenter;
	}

	@Override
	public boolean isOnline() {
		return false;
	}
}