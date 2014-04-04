package swift.crdt;

public class IncompatibleLockException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public IncompatibleLockException(String msg) {
        super(msg);
    }

}
