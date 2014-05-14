package swift.proto;

/**
 * A message that can provide a sample of metadata size it carries.
 * 
 * @author mzawirsk
 */
public interface MetadataMeasure {

    /**
     * @return metadata size statistics for this object
     */
    public abstract MetadataSizeSample getMetadataSizeSample();

}
