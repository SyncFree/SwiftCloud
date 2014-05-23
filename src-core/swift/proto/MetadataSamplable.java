package swift.proto;

/**
 * A message that can provide a sample of metadata size it carries.
 * 
 * @author mzawirsk
 */
public interface MetadataSamplable {

    /**
     * Records metadata size statistics for this object if the colletor is
     * enabled
     */
    public void recordMetadataSample(MetadataStatsCollector collector);

}
