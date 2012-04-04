package sys.net.api;

import java.io.DataInputStream;
import java.io.DataOutputStream;

/**
 * Provides basic serializer functionality. Allows objects to be converted into
 * arrays of bytes and back, or be sent and received from input streams.
 * 
 * @author smd
 * 
 */
public interface Serializer {

	/**
	 * Serializes an object to an array of bytes.
	 * 
	 * @param o
	 *            the object to be serialized
	 * @return the array of bytes representing the object
	 * @throws SerializerException
	 *             if the serialization fails
	 */
	byte[] writeObject(Object o) throws SerializerException;

	/**
	 * Converts the contents of an array of bytes into an object
	 * 
	 * @param data
	 *            the array containing the serialized representation of the
	 *            object
	 * @return the object, cast to the desired type.
	 * @throws SerializerException
	 */
	<T> T readObject(byte[] data) throws SerializerException;

	/**
	 * Serializes an object to a data output stream.
	 * 
	 * @param o
	 *            the object to be serialized
	 * @throws SerializerException
	 *             if the serialization fails
	 */
	void writeObject(DataOutputStream out, Object o) throws SerializerException;

	/**
	 * De-serializes an objec from a data input stream
	 * 
	 * @param in
	 *            The input stream that will be used to retrieve the object
	 *            representation
	 * @return the object, cast to the desired type.
	 * @throws SerializerException
	 *             if the de-serialization fails
	 */
	<T> T readObject(DataInputStream in) throws SerializerException;
}
