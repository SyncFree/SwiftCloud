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
package swift.client;

import swift.crdt.CRDTIdentifier;
import sys.stats.Stats;
import sys.stats.sources.CounterSignalSource;

/**
 * Statistics of Swift scout cache performance.
 * 
 * @author balegas
 */
public class CoarseCacheStats {
    private Stats stats;
    private CounterSignalSource cacheHits;
    private CounterSignalSource cacheMissNoObject;
    private CounterSignalSource cacheMissWrongVersion;
    private CounterSignalSource cacheMissBizarre;

    public CoarseCacheStats(Stats stats) {
        this.stats = stats;
        this.cacheHits = this.stats.getCountingSourceForStat("cache:hits");
        this.cacheMissNoObject = this.stats.getCountingSourceForStat("cache:miss-no-object");
        this.cacheMissWrongVersion = this.stats.getCountingSourceForStat("cache:miss-wrong-version");
        this.cacheMissBizarre = this.stats.getCountingSourceForStat("cache:miss-bizarre");
    }

    public void addCacheHit(final CRDTIdentifier id) {
        this.cacheHits.incCounter();
    }

    public void addCacheMissNoObject(final CRDTIdentifier id) {
        this.cacheMissNoObject.incCounter();
    }

    public void addCacheMissWrongVersion(final CRDTIdentifier id) {
        this.cacheMissWrongVersion.incCounter();
    }

    public void addCacheMissBizarre(final CRDTIdentifier id) {
        this.cacheMissBizarre.incCounter();
    }

    /**
     * Print statistics on stdout and resets the counters.
     */
    public void printAndReset() {
        // TODO: Implement? or remove?
    }

}
