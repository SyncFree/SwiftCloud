package swift.exceptions;

public class NoSuchObjectException extends SwiftException {
    /**
     * 
     */
    private static final long serialVersionUID = 5997340774968214678L;

    public NoSuchObjectException(Exception exception) {
        super(exception);
    }

    public NoSuchObjectException(String string) {
        super(string);
    }
}
