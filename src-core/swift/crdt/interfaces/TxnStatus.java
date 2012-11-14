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
package swift.crdt.interfaces;

/**
 * Status of a SwiftCloud transaction.
 * 
 * @author mzawirski
 */
// TODO: define legal transitions
public enum TxnStatus {
    /**
     * Open and accepts operations.
     */
    PENDING(true, false, false, false),
    /**
     * Cancelled. Does not accept new operations, previously performed
     * operations are lost.
     */
    CANCELLED(false, true, false, false),
    /**
     * Committed locally. Operations are visible to new local transactions, but
     * possibly not propagated to the store.
     */
    COMMITTED_LOCAL(false, true, true, false),
    /**
     * Committed to the store. Operations are (or will be soon) be visible to
     * transactions started at other clients.
     */
    COMMITTED_GLOBAL(false, true, true, true);

    private final boolean acceptingOps;
    private final boolean terminated;
    private final boolean outcomeLocallyVisible;
    private final boolean outcomePubliclyVisible;

    private TxnStatus(final boolean acceptingOps, final boolean terminated, final boolean outcomeLocallyVisible,
            final boolean outcomePubliclyVisible) {
        this.acceptingOps = acceptingOps;
        this.terminated = terminated;
        this.outcomeLocallyVisible = outcomeLocallyVisible;
        this.outcomePubliclyVisible = outcomePubliclyVisible;
    }

    /**
     * @return true if transaction accepts new operations
     */
    public boolean isAcceptingOperations() {
        return acceptingOps;
    }

    /**
     * @return true if transaction has terminated
     */
    public boolean isTerminated() {
        return terminated;
    }

    /**
     * @return true if outcome of a transaction is locally visible
     */
    public boolean isOutcomeLocallyVisible() {
        return outcomeLocallyVisible;
    }

    /**
     * @return true if outcome of a transaction is publicly visible (soft
     *         guarantee)
     */
    public boolean isOutcomePubliclyVisible() {
        return outcomePubliclyVisible;
    }
}
