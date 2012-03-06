package swift.client.proto;

/**
 * Rewrite in SwiftServer manner.
 * 
 * @author mzawirski
 */
public interface SwiftClient {
    boolean notifyNewUpdates(CRDTDelta delta);
}
