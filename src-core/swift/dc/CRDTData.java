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
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDT;

public class CRDTData<V extends CRDT<V>> {
    /**
     * true if the entry corresponds to an object that does not exist
     */
    boolean empty;
    /**
     * crdt object
     */
    CRDT<V> crdt;
    /**
     * crdt object
     */
    // CRDT<V> prunedCrdt;
    /**
     * CRDT unique identifier
     */
    CRDTIdentifier id;
    /**
     * current clock reflects all updates and their causal past
     */
    CausalityClock clock;
    /**
     * current clock reflects all updates and their causal past, from the
     * perspective of clients
     */
    // CausalityClock cltClock;
    /**
     * prune clock reflects the updates that have been discarded, making it
     * impossible to access a snapshot that is dominated by this clock
     */
    CausalityClock pruneClock;
    transient long lastPrunedTime;
    transient CausalityClock lastPrunedClock;
    transient Object dbInfo;

    CRDTData() {
        lastPrunedTime = -1;
    }

    CRDTData(CRDTIdentifier id) {
        lastPrunedTime = -1;
        this.id = id;
        this.empty = true;
    }

    CRDTData(CRDTIdentifier id, CRDT<V> crdt, CausalityClock clock, CausalityClock pruneClock, CausalityClock cltClock,
            int foo) {
        lastPrunedTime = -1;
        this.crdt = crdt;
        // if( DCDataServer.prune)
        // this.prunedCrdt = crdt.copy();
        this.id = id;
        this.clock = clock;
        this.pruneClock = pruneClock;
        this.pruneClock.trim();
        // this.cltClock = cltClock;
        this.empty = false;
    }

    CRDTData(CRDTIdentifier id, CRDT<V> crdt, CausalityClock clock, CausalityClock pruneClock, CausalityClock cltClock) {
        lastPrunedTime = -1;
        this.crdt = crdt;
        // if( DCDataServer.prune)
        // this.prunedCrdt = crdt.copy();
        this.id = id;
        this.clock = clock;
        this.pruneClock = pruneClock;
        this.pruneClock.trim();
        // this.cltClock = cltClock;
        this.empty = false;
    }

    boolean pruneIfPossible() {
        long curTime = System.currentTimeMillis();
        if (lastPrunedTime == -1) {
            lastPrunedTime = curTime;
            lastPrunedClock = (CausalityClock) clock.copy();
            lastPrunedClock.trim();
        }
        if (lastPrunedTime + DCConstants.PRUNING_INTERVAL < curTime) {
            crdt.prune(lastPrunedClock, false);
            pruneClock = lastPrunedClock;
            lastPrunedTime = curTime;
            lastPrunedClock = (CausalityClock) clock.copy();
            lastPrunedClock.trim();
            return true;
        } else
            return false;

    }

    void initValue(CRDT<V> crdt, CausalityClock clock, CausalityClock pruneClock, CausalityClock cltClock, int foo) {
        this.crdt = crdt;
        // if( DCDataServer.prune)
        // this.prunedCrdt = crdt.copy();
        this.clock = clock;
        this.pruneClock = pruneClock;
        // this.cltClock = cltClock;
        this.empty = false;
    }

    void initValue(CRDT<V> crdt, CausalityClock clock, CausalityClock pruneClock, CausalityClock cltClock) {
        this.crdt = crdt;
        // if( DCDataServer.prune)
        // this.prunedCrdt = crdt.copy();
        this.clock = clock;
        this.pruneClock = pruneClock;
        // this.cltClock = cltClock;
        this.empty = false;
    }

    public int hashCode() {
        return id.hashCode();
    }

    public boolean equals(Object obj) {
        return obj instanceof CRDTData && id.equals(((CRDTData) obj).id);

    }

    public Object getDbInfo() {
        return dbInfo;
    }

    public void setDbInfo(Object dbInfo) {
        this.dbInfo = dbInfo;
    }

    public void mergeInto(CRDTData<?> d) {
        empty = true;
        crdt.merge((CRDT<V>) d.crdt);
        clock.merge(d.clock);
        // if( DCDataServer.prune) {
        // this.prunedCrdt.merge((CRDT<V>)d.crdt);
        // }
        pruneClock.merge(d.pruneClock);
        // cltClock.merge(d.cltClock);
    }

    public boolean isEmpty() {
        return empty;
    }

    public CRDT<V> getCrdt() {
        return crdt;
    }

    public CRDTIdentifier getId() {
        return id;
    }

    public CausalityClock getClock() {
        return clock;
    }

    public CausalityClock getPruneClock() {
        return pruneClock;
    }

    /*
     * public CausalityClock getCltClock() { return cltClock; }
     */
}
