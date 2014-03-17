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
package swift.crdt.core;

/**
 * Definition of isolation levels offered to a transaction, i.e. guarantees for
 * reads made by a transaction.
 * 
 * @author mzawirski
 */
public enum IsolationLevel {
    /**
     * Transaction reads from a consistent snapshot defined at the beginning of
     * the transaction. Transaction observes results of all previously locally
     * committed transactions (read-your-writes) and all previous reads
     * (monotonic-reads)
     * <p>
     * Note that snapshots of different transactions are only partially ordered,
     * not totally ordered. I.e., formally this is Non-Mononotonic Snapshot
     * Isolation.
     */
    SNAPSHOT_ISOLATION,
    /**
     * Transaction reads from a snapshot which may be inconsistent. Reading the
     * same object twice (through
     * {@link TxnHandle#get(swift.crdt.CRDTIdentifier, boolean, Class)}) yields
     * the same result.
     * <p>
     * The system makes best effort to make sure that the transaction observe as
     * much of previously locally committed transactions (read-your-writes) and
     * all previous reads (monotonic-reads) as possible, preferably all.
     */
    // Why only best-effort? Because of potential cache entry eviction and
    // concurrent read request by application.
    REPEATABLE_READS,
    /**
     * Transaction reads from committed transactions. Same as
     * {@link #REPEATABLE_READS}, but reading the same object twice may yield
     * different results.
     */
    // TODO: implement
    READ_COMMITTED,
    /**
     * Transaction reads may observe only partial results of some transaction.
     */
    // TODO: implement
    READ_UNCOMMITTED;
}
