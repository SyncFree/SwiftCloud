package swift.client.proto;

/**
 * TODO: complete, adapt to RPC library (error handling etc.), document.
 * 
 * @author mzawirski
 */
public interface SwiftServer {
    FetchObjectDeltaReply fetchObjectDelta(FetchObjectDeltaRequest request);

    FetchObjectVersionReply fetchObjectVersion(FetchObjectVersionRequest request);

    GenerateTimestampRequest generateTimestamp(GenerateTimestampRequest request);

    KeepaliveRequest keepalive(KeepaliveRequest request);

    void unsubscribeRequest(UnsubscribeRequest request);

    BlockingCommitReply blockingTranslateAndCommit(BlockingCommitRequest request);

    SubmitUpdatesReply submitUpdates(SubmitUpdatesRequest request);
}
