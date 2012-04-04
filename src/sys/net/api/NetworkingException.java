package sys.net.api;

/**
 * 
 * A generic exception for wrapping assorted networking-related errors.
 * 
 * @author smd
 * 
 */
public class NetworkingException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public NetworkingException(final String cause) {
		super(cause);
	}

	public NetworkingException(final Throwable t) {
		super(t);
	}
}
