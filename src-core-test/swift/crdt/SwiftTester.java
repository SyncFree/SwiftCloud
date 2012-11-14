/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package swift.crdt;

import swift.clocks.CausalityClock;
import swift.clocks.ClockFactory;
import swift.clocks.IncrementalTimestampGenerator;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.SwiftScout;
import swift.crdt.interfaces.SwiftSession;
import swift.crdt.interfaces.TxnHandle;

public class SwiftTester implements SwiftSession {
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
        return new TxnTester(id, latestVersion, clientTimestampGenerator.generateNew(),
                globalTimestampGenerator.generateNew());
    }

    public <V extends CRDT<V>> TxnTesterWithRegister beginTxn(V target) {
        return new TxnTesterWithRegister(id, latestVersion, clientTimestampGenerator.generateNew(),
                globalTimestampGenerator.generateNew(), target);
    }

    public <V extends CRDT<V>> void merge(V local, V other, SwiftTester otherSwift) {
        local.merge(other);
        latestVersion.merge(otherSwift.latestVersion);
    }

    public <V extends CRDT<V>> void prune(V local, CausalityClock c) {
        local.prune(c.clone(), true);
    }

    @Override
    public void stopScout(boolean waitForCommit) {
    }

    @Override
    public String getSessionId() {
        return "tester-session";
    }

    @Override
    public SwiftScout getScout() {
        // TODO Auto-generated method stub
        return null;
    }
}
