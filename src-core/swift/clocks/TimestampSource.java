package swift.clocks;

/**
 * Source for generating new timestamps.
 * 
 * @param T type of timestamps
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
