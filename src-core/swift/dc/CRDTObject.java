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
package swift.dc;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.interfaces.CRDT;

public class CRDTObject<V extends CRDT<V>> {
    /**
     * crdt object
     */
    CRDT<V> crdt;
    /**
     * current clock reflects all updates and their causal past
     */
    CausalityClock clock;
    /**
     * prune clock reflects the updates that have been discarded, making it
     * impossible to access a snapshot that is dominated by this clock
     */
    CausalityClock pruneClock;

    public CRDTObject() {
        // do nothing
    }

    public CRDTObject(CRDTData<V> data, CausalityClock version, String cltId, CausalityClock cltClock) {
        // 1) let int clientTxs =
        // clientTxClockService.getAndLockNumberOfCommitedTxs(clientId) //
        // probably it could be done better, lock-free
        // 2)
        // let crdtCopy = retrieve(oid).copy()
        // crdtCopy.augumentWithScoutClock(new Timestamp(clientId, clientTxs))
        // 3) clientTxClockService.unlock(clientId)
        // 4) return crdtCopy
        /*
         * if( DCDataServer.prune) { CMP_CLOCK cmp = version.compareTo(
         * data.pruneClock); if( cmp == CMP_CLOCK.CMP_EQUALS || cmp ==
         * CMP_CLOCK.CMP_DOMINATES) this.crdt = data.prunedCrdt.copy(); else
         * this.crdt = data.crdt.copy(); } else;
         */
        this.crdt = data.crdt.copy();
        this.clock = data.clock.clone();
        this.pruneClock = data.pruneClock.clone();
        Timestamp ts = null;
        synchronized (cltClock) {
            ts = cltClock.getLatest(cltId);
        }
        if (ts != null && ts.getCounter() > 0)
            this.clock.recordAllUntil(ts);
    }
}