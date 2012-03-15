package swift.exceptions;

public class ConsistentSnapshotVersionNotFoundException extends SwiftException {
    /**
     * 
     */
    private static final long serialVersionUID = 1231199243385467538L;

    public ConsistentSnapshotVersionNotFoundException(String message) {
        super(message);
    }
}
