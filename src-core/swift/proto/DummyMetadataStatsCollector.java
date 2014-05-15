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
    public Output getKryoBuffer() {
        return null;
    }

    @Override
    public Kryo getKryo() {
        return null;
    }

    @Override
    public void recordStats(Object message, int totalSize, int objectOrUpdateSize, int objectOrUpdateValueSize) {
        // no-op
    }
}
