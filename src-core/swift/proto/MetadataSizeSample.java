package swift.proto;

/**
 * A sample of metadata size for a message.
 * 
 * @author mzawirski
 */
public class MetadataSizeSample {
    private String messageName;
    // TODO: we should intercept totalSize at the serialization time rather than
    // forcing re-serialization for measurements purposes
    private int totalSize;
    private int objectOrUpdateSize;
    private int objectOrUpdateValueSize;

    /**
     * Creates a sample. Assumption: totalSize >= objectOrUpdateSize >=
     * objectOrUpdateValueSize
     * 
     * @param messageName
     *            name of the message
     * @param totalSize
     *            total size of the encoded message
     * @param objectOrUpdateSize
     *            total size of the object version or the object update, with
     *            type-specific metadata
     * @param objectOrUpdateValueSize
     *            total size of the value carried in the object version or the
     *            update (e.g., value of a counter)
     */
    public MetadataSizeSample(String messageName, int totalSize, int objectOrUpdateSize, int objectOrUpdateValueSize) {
        this.messageName = messageName;
        this.totalSize = totalSize;
        this.objectOrUpdateSize = objectOrUpdateSize;
        this.objectOrUpdateValueSize = objectOrUpdateValueSize;
    }

    @Override
    public String toString() {
        return String.format("MetadataSample,%s,%d,%d,%d", messageName, totalSize, objectOrUpdateSize,
                objectOrUpdateValueSize);
    }
}
