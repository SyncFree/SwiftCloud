package sys.net.api;

/**
 * 
 * A generic exception for wrapping assorted serialization errors. Currently
 * unchecked...
 * 
 * @author smd (smd@fct.unl.pt)
 * 
 */
public class SerializerException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public SerializerException(final String cause) {
		super(cause);
	}

	public SerializerException(final Throwable t) {
		super(t);
	}
}
