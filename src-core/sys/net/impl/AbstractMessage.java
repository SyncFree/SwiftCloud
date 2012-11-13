package sys.net.impl;

import sys.net.api.Message;

abstract public class AbstractMessage implements Message {
	
	/*
	 * TODO prevent this from being serialized...
	 */
	transient protected int size;

	/**
	 * Stores the size of the message once it has been serialized.
	 * @param the size of the serialized representation of the message in bytes.
	 */
	public void setSize( int size ) {
		this.size = size;
	}
	
	/**
	 * Returns the size of the message after being serialized.
	 * @return the size of the serialized representation of the message in bytes.
	 */
	public int getSize() {
		return this.size;
	}
}
