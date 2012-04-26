package swift.crdt.interfaces;

/**
 * Interface for deep copy operation.
 * 
 * @author annettebieniusa
 * 
 * @param <V>
 */
public interface Copyable {
    /**
     * Creates a deep copy of the object.
     * 
     * @return object copy
     */
    Object copy();

}
