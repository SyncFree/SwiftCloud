package swift.proto;

import swift.utils.SafeLog;
import swift.utils.SafeLog.ReportType;
import sys.net.impl.KryoLib;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.Output;

/**
 * A collector of metadata statistics.
 * 
 * @author mzawirski
 */
public class MetadataStatsCollector {
    public final static int EXPECTED_BUFFER_SIZE = 2 << 15;
    public static final int MAX_BUFFER_SIZE = 2 << 23;

    public static String defaultMessageName(Object message) {
        return message.getClass().getSimpleName();
    }

    private String nodeId;
    private ThreadLocal<Output> freshKryoBuffer = new ThreadLocal<Output>() {
        protected Output initialValue() {
            return new ByteBufferOutput(EXPECTED_BUFFER_SIZE, MAX_BUFFER_SIZE);
        }
    };

    /**
     * Creates a collector.
     */
    public MetadataStatsCollector(final String nodeId) {
        this.nodeId = nodeId;
    }

    public boolean isMessageReportEnabled() {
        return ReportType.METADATA.isEnabled();
    }

    /**
     * @return a thread-local cleared kryo buffer that can be used to compute
     *         the size of messages
     */
    public Output getFreshKryoBuffer() {
        Output buffer = freshKryoBuffer.get();
        if (buffer.position() < EXPECTED_BUFFER_SIZE) {
            // Reuse an existing buffer.
            buffer.clear();
        } else {
            // Lower memory footprint by creating a fresh buffer.
            freshKryoBuffer.remove();
            buffer = freshKryoBuffer.get();
        }
        return buffer;
    }

    /**
     * @return a Kryo instance that can be used to compute the size of messages
     */
    public Kryo getFreshKryo() {
        final Kryo kryo = KryoLib.kryoWithoutAutoreset();
        kryo.reset();
        return kryo;
    }

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
    public void recordMessageStats(Object message, int totalSize, int objectOrUpdateSize, int objectOrUpdateValueSize,
            int batchIndependentGlobalMetadataSize, int batchDependentGlobalMetadataSize, int batchSizeFinestGrained,
            int batchSizeFinerGrained, int batchSizeCoarseGrained, int maxVectorSize, int maxExceptionsNum) {
        recordMessageStats(defaultMessageName(message), totalSize, objectOrUpdateSize, objectOrUpdateValueSize,
                batchIndependentGlobalMetadataSize, batchDependentGlobalMetadataSize, batchSizeFinestGrained,
                batchSizeFinerGrained, batchSizeCoarseGrained, maxVectorSize, maxExceptionsNum);
    }

    public void recordMessageStats(String messageName, int totalSize, int objectOrUpdateSize,
            int objectOrUpdateValueSize, int batchIndependentGlobalMetadataSize, int batchDependentGlobalMetadataSize,
            int batchSizeFinestGrained, int batchSizeFinerGrained, int batchSizeCoarseGrained, int maxVectorSize,
            int maxExceptionsNum) {
        // TODO: we should intercept totalSize at the serialization time rather
        // than forcing re-serialization for measurements purposes
        if (isMessageReportEnabled()) {
            SafeLog.report(ReportType.METADATA, nodeId, messageName, totalSize, objectOrUpdateSize,
                    objectOrUpdateValueSize, batchIndependentGlobalMetadataSize, batchDependentGlobalMetadataSize,
                    batchSizeFinestGrained, batchSizeFinerGrained, batchSizeCoarseGrained, maxVectorSize,
                    maxExceptionsNum);
        }
    }

    public boolean isDatabaseTableReportEnabled() {
        return ReportType.DATABASE_TABLE_SIZE.isEnabled();
    }

    public void recordDatabaseTableStats(String tableName, int size) {
        if (isDatabaseTableReportEnabled()) {
            SafeLog.report(ReportType.DATABASE_TABLE_SIZE, nodeId, tableName, size);
        }
    }
}