package swift.client.proto;

import java.util.LinkedList;
import java.util.List;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

/**
 * Client batch request to commit a a sequence of transactions. Transactions
 * should be committed sequentially and a depedency clock of <em>i+1</em>-th
 * should be updated to include system timestamp of <em>i</em>-th transaction.
 * 
 * @author mzawirski
 */
public class BatchCommitUpdatesRequest extends ClientRequest {
    protected LinkedList<CommitUpdatesRequest> commitRequests;

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    BatchCommitUpdatesRequest() {
    }

    /**
     * Creates a batch commit request made of provided list of requests.
     * 
     * @param clientId
     *            client id
     * @param commitRequests
     *            sequence of commit requests from the client
     */
    public BatchCommitUpdatesRequest(String clientId, List<CommitUpdatesRequest> commitRequests) {
        super(clientId);
        this.commitRequests = new LinkedList<CommitUpdatesRequest>(commitRequests);
    }

    /**
     * @return sequence of commit updates requests, one possibly dependent on
     *         another
     */
    // TODO: Weaken the dependencies?
    public LinkedList<CommitUpdatesRequest> getCommitRequests() {
        return commitRequests;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((SwiftServer) handler).onReceive(conn, this);
    }

}
