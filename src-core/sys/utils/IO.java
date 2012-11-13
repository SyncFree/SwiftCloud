package sys.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.channels.Channel;

public class IO {

	protected IO() {};
	
	
	public static void close( Socket s ) {
		try {
			s.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void close( InputStream in ) {
		try {
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void close( OutputStream out ) {
		try {
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void close( Channel ch ) {
		try {
			ch.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
