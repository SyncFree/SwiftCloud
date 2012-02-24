package swift.exceptions;

@SuppressWarnings("serial")
public class IncompatibleTypeException extends Exception {
    public IncompatibleTypeException(final Exception e) {
        super(e);
    }

    public IncompatibleTypeException() {
        super();
    }
}
