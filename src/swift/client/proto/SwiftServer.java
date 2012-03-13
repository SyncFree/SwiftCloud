package swift.client.proto;

import swift.clocks.CausalityClock;

/**
 * Server interface for client-server interaction.
 * <p>
 * This interface defines interaction between clients and a single server, i.e.
 * sessions with affinity. Server can identify originating client using
 * Catadupa/RPC primitives available in messages. For documentation of
 * particular requests, see message definitions.
 * 
 * TODO: adapt to RPC library (specify error handling, blocking/nonblocking
 * etc.)
 * 
 * @author mzawirski
 */
public interface SwiftServer {
    FetchObjectVersionReply fetchObjectVersion(FetchObjectVersionRequest request);

    // TODO: FetchObjectVersionReply is temporary. Eventually, we shall replace
    // it with deltas or list of operations.
    FetchObjectVersionReply fetchObjectDelta(FetchObjectDeltaRequest request);

    GenerateTimestampReply generateTimestamp(GenerateTimestampRequest request);

    KeepaliveRequest keepalive(KeepaliveRequest request);

    void unsubscribeNotifications(UnsubscribeNotificationsRequest request);

    CommitUpdatesReply commitUpdates(CommitUpdatesRequest request);

    CausalityClock getLatestKnownClock(LatestKnownClockRequest request);
}
