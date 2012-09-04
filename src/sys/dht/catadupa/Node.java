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