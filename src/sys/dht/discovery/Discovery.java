package sys.dht.discovery;

import java.io.IOException;
import java.net.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import sys.net.api.Endpoint;
import sys.net.api.Serializer;
import sys.utils.Threading;

import static sys.utils.Log.*;
import static sys.net.api.Networking.*;

/**
 * 
 * Uses multicast communication to discover a Catadupa/DHT seed node in the
 * local machine/network...
 * 
 * Used as a zero configuration debug convenience when running in a single
 * machine or local network.
 * 
 * @author Sérgio Duarte (smd@fct.unl.pt)
 * 
 */
public class Discovery implements Runnable {
	private static final String GROUP_PORT = "9099";
	private static final String GROUP_ADDR = "239.239.239.239";

	private int reg_port;
	private MulticastSocket cs;
	private InetAddress group_addr;
	private Serializer serializer;

	private Map<String, Endpoint> registry;;

	Discovery() throws Exception {
		this.cs = new MulticastSocket();
		reg_port = Integer.parseInt(System.getProperty("port") == null ? GROUP_PORT : System.getProperty("port"));
		this.group_addr = InetAddress.getByName(System.getProperty("group") == null ? GROUP_ADDR : System.getProperty("group"));
		serializer = Networking.serializer();
	}

	public void run() {
		try {
			MulticastSocket ss = new MulticastSocket(reg_port);
			ss.setReuseAddress(true);
			ss.joinGroup(group_addr);
			for (;;) {
				try {
					DatagramPacket req = new DatagramPacket(new byte[65536], 65536);
					ss.receive(req);
					Endpoint ep = registry.get(readObject(req));
					if (ep != null) {
						byte[] replyData = serializer.writeObject(ep);
						DatagramPacket reply = new DatagramPacket(replyData, replyData.length, req.getSocketAddress());
						ss.send(reply);
					}
				} catch (IOException x) {
					x.printStackTrace();
				}
			}
		} catch (IOException x) {
			x.printStackTrace();
		}
	}

	private Endpoint doLookup(String key, int timeout, int ttl) {
		byte[] requestData = serializer.writeObject(key);
		DatagramPacket request = new DatagramPacket(requestData, requestData.length, group_addr, reg_port);
		long deadline = now() + timeout;
		while (now() < deadline) {
			try {
				cs.setTimeToLive(ttl);
				cs.send(request);
				cs.setSoTimeout((int) Math.min(500, deadline - now()));
				DatagramPacket reply = new DatagramPacket(new byte[65536], 65536);
				cs.receive(reply);
				Endpoint remote = readObject(reply);
				Log.finest(String.format("Discovered: %s at %s in %d ms", key, remote, now() - (deadline - timeout)));
				return remote;
			} catch (SocketTimeoutException x) {
			} catch (IOException x) {
				x.printStackTrace();
			}
		}
		return null;
	}

	public static Endpoint lookup(String key, int timeout, int ttl) {
		return getInstance().doLookup(key, timeout, ttl);
	}

	public static Endpoint lookup(String key, int timeout) {
		return getInstance().doLookup(key, timeout, 1);
	}

	synchronized public static void register(String key, Endpoint endpoint) {
		if (getInstance().registry == null) {
			getInstance().registry = new HashMap<String, Endpoint>();
			Threading.newThread(true, getInstance()).start();
		}
		getInstance().registry.put(key, endpoint);
	}

	private static long now() {
		return System.currentTimeMillis();
	}

	synchronized static Discovery getInstance() {
		if (singleton == null) {
			try {
				singleton = new Discovery();

			} catch (Throwable e) {
				e.printStackTrace();
			}
		}
		return singleton;
	}

	@SuppressWarnings("unchecked")
	private <T> T readObject(DatagramPacket p) {
		return (T) serializer.readObject(Arrays.copyOf(p.getData(), p.getLength()));
	}

	private static Discovery singleton;
}
