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
public class MetadataStatsCollectorImpl implements MetadataStatsCollector {
    public final static int EXPECTED_BUFFER_SIZE = 2 << 15;
    public static final int MAX_BUFFER_SIZE = 2 << 23;
    private String nodeId;
    private ThreadLocal<Output> freshKryoBuffer = new ThreadLocal<Output>() {
        protected Output initialValue() {
            return new ByteBufferOutput(EXPECTED_BUFFER_SIZE, MAX_BUFFER_SIZE);
        }
    };

    /**
     * Creates a collector.
     */
    public MetadataStatsCollectorImpl(final String nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public boolean isMessageReportEnabled() {
        return ReportType.METADATA.isEnabled();
    }

    @Override
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

    @Override
    public Kryo getFreshKryo() {
        final Kryo kryo = KryoLib.kryoWithoutAutoreset();
        kryo.reset();
        return kryo;
    }

    @Override
    public void recordMessageStats(Object message, int totalSize, int objectOrUpdateSize, int objectOrUpdateValueSize,
            int explicitlyComputedGlobalMetadataSize, int batchSize, int maxVectorSize, int maxExceptionsNum) {
        // TODO: we should intercept totalSize at the serialization time rather
        // than forcing re-serialization for measurements purposes
        if (isMessageReportEnabled()) {
            SafeLog.report(ReportType.METADATA, nodeId, message.getClass().getSimpleName(), totalSize,
                    objectOrUpdateSize, objectOrUpdateValueSize, explicitlyComputedGlobalMetadataSize, batchSize,
                    maxVectorSize, maxExceptionsNum);
        }
    }

    @Override
    public boolean isDatabaseTableReportEnabled() {
        return ReportType.DATABASE_TABLE_SIZE.isEnabled();
    }

    @Override
    public void recordDatabaseTableStats(String tableName, int size) {
        if (isDatabaseTableReportEnabled()) {
            SafeLog.report(ReportType.DATABASE_TABLE_SIZE, nodeId, tableName, size);
        }
    }
}
