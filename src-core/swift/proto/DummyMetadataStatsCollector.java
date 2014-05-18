package swift.proto;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

/**
 * A permanently disabled, "dummy", metadata statistics collector.
 * 
 * @author mzawirski
 */
public class DummyMetadataStatsCollector implements MetadataStatsCollector {

    @Override
    public boolean isEnabled() {
        return false;
    }

    @Override
    public Output getFreshKryoBuffer() {
        return null;
    }

    @Override
    public Kryo getFreshKryo() {
        return null;
    }

    @Override
    public void recordStats(Object message, int totalSize, int objectOrUpdateSize, int objectOrUpdateValueSize,
            int batchSize, int maxExceptionsNum) {
        // no-op
    }
}
