package swift.proto;

import swift.utils.SafeLog;
import swift.utils.SafeLog.ReportType;
import sys.net.impl.KryoLib;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.UnsafeMemoryOutput;

/**
 * A collector of metadata statistics.
 * 
 * @author mzawirski
 */
public class MetadataStatsCollectorImpl implements MetadataStatsCollector {
    private String scoutId;
    private ThreadLocal<UnsafeMemoryOutput> freshKryoBuffer = new ThreadLocal<UnsafeMemoryOutput>() {
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
    public MetadataStatsCollectorImpl(final String scoutId) {
        this.scoutId = scoutId;
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public Output getFreshKryoBuffer() {
        Output buffer = freshKryoBuffer.get();
        buffer.clear();
        return buffer;
    }

    @Override
    public Kryo getFreshKryo() {
        final Kryo kryo = KryoLib.kryoWithoutAutoreset();
        kryo.reset();
        return kryo;
    }

    @Override
    public void recordStats(Object message, int totalSize, int objectOrUpdateSize, int objectOrUpdateValueSize,
            int explicitlyComputedGlobalMetadataSize, int batchSize, int maxVectorSize, int maxExceptionsNum) {
        // TODO: we should intercept totalSize at the serialization time rather
        // than forcing re-serialization for measurements purposes
        if (isEnabled()) {
            SafeLog.report(ReportType.METADATA, scoutId, message.getClass().getSimpleName(), totalSize,
                    objectOrUpdateSize, objectOrUpdateValueSize, explicitlyComputedGlobalMetadataSize, batchSize,
                    maxVectorSize, maxExceptionsNum);
        }
    }
}
