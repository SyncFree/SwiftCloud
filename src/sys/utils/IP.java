package sys.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;

import sys.net.api.NetworkingException;

public class IP {
	
	public static InetAddress localHostAddress() {
		try {
			return InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			throw new NetworkingException( e.getMessage() );
		}
	}
	
	public static String localHostAddressString() {
		try {
			return InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			throw new NetworkingException( e.getMessage() );
		}
	}
}
