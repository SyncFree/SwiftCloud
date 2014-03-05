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

import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.SwiftScout;
import swift.crdt.interfaces.SwiftSession;
import swift.crdt.interfaces.TxnHandle;
import swift.exceptions.NetworkException;

/**
 * Adapter of (possibly) shared Swift scout instance into a single session view,
 * using unique sessionId;
 * 
 * @author mzawirski
 */
class SwiftSessionToScoutAdapter implements SwiftSession {
    private final SwiftImpl sharedSwift;
    private final String sessionId;

    public SwiftSessionToScoutAdapter(SwiftImpl swiftImpl, String sessionId) {
        this.sharedSwift = swiftImpl;
        this.sessionId = sessionId;
    }

    @Override
    public TxnHandle beginTxn(IsolationLevel isolationLevel, CachePolicy cachePolicy, boolean readOnly)
            throws NetworkException {
        return sharedSwift.beginTxn(sessionId, isolationLevel, cachePolicy, readOnly);
    }

    @Override
    public void stopScout(boolean waitForCommit) {
        sharedSwift.stop(waitForCommit);
    }

    @Override
    public String getSessionId() {
        return sessionId;
    }

    @Override
    public SwiftScout getScout() {
        return sharedSwift;
    }

    @Override
    public void printStatistics() {
        sharedSwift.printAndResetCacheStats();
    }
}
