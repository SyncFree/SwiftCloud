package swift.crdt.interfaces;


/**
 * Source for generating new timestamps.
 * 
 * @author nmp
 */
public interface TimestampSource<T> {


    /**
     * Generates a new timestamp.
     * 
     * @return
     */
    T generateNew();

}
