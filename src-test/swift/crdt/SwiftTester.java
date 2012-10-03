package swift.crdt;

import swift.clocks.CausalityClock;
import swift.clocks.ClockFactory;
import swift.clocks.IncrementalTimestampGenerator;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.Swift;
import swift.crdt.interfaces.TxnHandle;

public class SwiftTester implements Swift {
    public CausalityClock latestVersion;
    private IncrementalTimestampGenerator clientTimestampGenerator;
    private IncrementalTimestampGenerator globalTimestampGenerator;
    private String id;

    public SwiftTester(String id) {
        this.id = id;
        this.latestVersion = ClockFactory.newClock();
        this.clientTimestampGenerator = new IncrementalTimestampGenerator(id);
        this.globalTimestampGenerator = new IncrementalTimestampGenerator("global:" + id);
    }

    @Override
    public TxnHandle beginTxn(IsolationLevel isolationLevel, CachePolicy cp, boolean readOnly) {
        throw new RuntimeException("Not implemented");
    }

    public TxnTester beginTxn() {
        return new TxnTester(id, latestVersion, clientTimestampGenerator.generateNew(), globalTimestampGenerator.generateNew());
    }

    public <V extends CRDT<V>> void merge(V local, V other, SwiftTester otherSwift) {
        local.merge(other);
        latestVersion.merge(otherSwift.latestVersion);
    }

    public <V extends CRDT<V>> void prune(V local, CausalityClock c) {
        local.prune(c.clone(), true);
    }

    @Override
    public void stop(boolean waitForCommit) {
    }
}
