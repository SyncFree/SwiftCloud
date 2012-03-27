package swift.test.crdt;

import swift.clocks.CausalityClock;
import swift.clocks.ClockFactory;
import swift.clocks.IncrementalTimestampGenerator;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.Swift;
import swift.crdt.interfaces.TxnHandle;

public class SwiftTester implements Swift {
    public CausalityClock latestVersion;
    private IncrementalTimestampGenerator clientTimestampGenerator;
    private String id;

    public SwiftTester(String id) {
        this.id = id;
        this.latestVersion = ClockFactory.newClock();
        this.clientTimestampGenerator = new IncrementalTimestampGenerator(id);
    }

    @Override
    public TxnHandle beginTxn(CachePolicy cp, boolean readOnly) {
        return new TxnTester(id, latestVersion, clientTimestampGenerator.generateNew());
    }

}
