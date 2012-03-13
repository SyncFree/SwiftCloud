package swift.client.proto;

/**
 * Server confirmation of committed updates.
 * 
 * @author mzawirski
 * @see CommitUpdatesRequest
 */
public class CommitUpdatesReply {
    protected boolean success;

    /**
     * @return true when transaction was successfully comitted; otherwise TODO:
     *         what?? what can be more fine grained errors?
     */
    public boolean isSuccess() {
        return success;
    }
}
