package swift.exceptions;

public class SwiftException extends Exception {
    public SwiftException(String message) {
        super(message);
    }

    public SwiftException(Exception exception) {
        super(exception);
    }

    private static final long serialVersionUID = -4007786119262519915L;

}
