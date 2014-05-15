package swift.proto;

import java.io.PrintStream;

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
    private ThreadLocal<UnsafeMemoryOutput> kryoBuffer = new ThreadLocal<UnsafeMemoryOutput>() {
        protected UnsafeMemoryOutput initialValue() {
            return new UnsafeMemoryOutput(1 << 20);
        }
    };
    private PrintStream stream;

    /**
     * Creates a collector.
     * 
     * @param enabled
     *            true if the collection is active
     */
    public MetadataStatsCollectorImpl(final String scoutId, PrintStream stream) {
        this.scoutId = scoutId;
        this.stream = stream;
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public Output getKryoBuffer() {
        Output buffer = kryoBuffer.get();
        buffer.clear();
        return buffer;
    }

    @Override
    public Kryo getKryo() {
        return KryoLib.kryo();
    }

    @Override
    public void recordStats(Object message, int totalSize, int objectOrUpdateSize, int objectOrUpdateValueSize) {
        // TODO: we should intercept totalSize at the serialization time rather
        // than forcing re-serialization for measurements purposes
        if (isEnabled()) {
            stream.printf("%s,MetadataSample,%s,%d,%d,%d\n", scoutId, message.getClass().getSimpleName(), totalSize,
                    objectOrUpdateSize, objectOrUpdateValueSize);
        }
    }
}
