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
import swift.crdt.core.CRDT;
import swift.crdt.core.CachePolicy;
import swift.crdt.core.IsolationLevel;
import swift.crdt.core.SwiftScout;
import swift.crdt.core.SwiftSession;
import swift.crdt.core.TxnHandle;
import swift.crdt.core.ManagedCRDT;

public class SwiftTester implements SwiftSession  {
    public static <V extends CRDT<V>> void prune(ManagedCRDT<V> local, CausalityClock c) {
        local.prune(c.clone(), true);
    }

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

    public TxnTester beginTxn(final ManagedCRDT<?>... existingObjects) {
        return new TxnTester(id, latestVersion, clientTimestampGenerator.generateNew(),
                globalTimestampGenerator.generateNew(), existingObjects);
    }

    public <V extends CRDT<V>> void merge(ManagedCRDT<V> local, ManagedCRDT<V> other, SwiftTester otherSwift) {
        local.merge(other);
        latestVersion.merge(other.getClock());
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
        return null;
    }

    @Override
    public void printStatistics() {
    }
}
