package swift.client;

import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
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
}
