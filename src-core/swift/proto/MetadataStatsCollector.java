package swift.proto;

import sys.net.impl.KryoLib;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.UnsafeMemoryOutput;

/**
 * A collector of metadata statistics.
 * 
 * @author mzawirski
 */
public class MetadataStatsCollector {
    private boolean enabled;
    private ThreadLocal<UnsafeMemoryOutput> kryoBuffer = new ThreadLocal<UnsafeMemoryOutput>() {
        protected UnsafeMemoryOutput initialValue() {
            return new UnsafeMemoryOutput(1 << 20);
        }
    };

    /**
     * Creates a collector.
     * 
     * @param enabled
     *            true if the collection is active
     */
    public MetadataStatsCollector(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isEnabled() {
        return enabled;
    }

    /**
     * @return a thread-local cleared kryo buffer that can be used to compute
     *         the size of messages
     */
    public Output getKryoBuffer() {
        Output buffer = kryoBuffer.get();
        buffer.clear();
        return buffer;
    }

    /**
     * @return a Kryo instance that can be used to compute the size of messages
     */
    public Kryo getKryo() {
        return KryoLib.kryo();
    }

    /**
     * Records a stats sample. Assumption: totalSize >= objectOrUpdateSize >=
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
    public void recordStats(String messageName, int totalSize, int objectOrUpdateSize, int objectOrUpdateValueSize) {
        // TODO: we should intercept totalSize at the serialization time rather
        // than forcing re-serialization for measurements purposes
        if (isEnabled()) {
            System.out.printf("MetadataSample,%s,%d,%d,%d\n", messageName, totalSize, objectOrUpdateSize,
                    objectOrUpdateValueSize);
        }
    }
}
