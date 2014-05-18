package swift.proto;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

public interface MetadataStatsCollector {

    public abstract boolean isEnabled();

    /**
     * @return a thread-local cleared kryo buffer that can be used to compute
     *         the size of messages
     */
    public abstract Output getFreshKryoBuffer();

    /**
     * @return a Kryo instance that can be used to compute the size of messages
     */
    public abstract Kryo getFreshKryo();

    /**
     * Records a stats sample for a message. Assumption: totalSize >=
     * objectOrUpdateSize >= objectOrUpdateValueSize
     * 
     * @param message
     *            message that the stats concern
     * @param totalSize
     *            total size of the encoded message
     * @param objectOrUpdateSize
     *            total size of the object version or the object update, with
     *            type-specific metadata
     * @param objectOrUpdateValueSize
     *            total size of the value carried in the object version or the
     *            update (e.g., value of a counter)
     * @param batchSize
     *            size of the batch (i.e., number of objects/updates) if
     *            applicable; otherwise 1
     * @param maxExceptionsNum
     *            maximum number of exceptions in a vector in the message
     */
    public abstract void recordStats(Object message, int totalSize, int objectOrUpdateSize,
            int objectOrUpdateValueSize, int batchSize, int maxExceptionsNum);
}