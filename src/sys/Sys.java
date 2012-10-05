package sys;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import swift.utils.KryoCRDTUtils;
import sys.dht.DHT_Node;
import sys.dht.api.DHT;
import sys.dht.catadupa.KryoCatadupa;
import sys.net.impl.NetworkingImpl;
import sys.scheduler.TaskScheduler;
import sys.utils.IP;

public class Sys {
	public static Logger SysLog = Logger.getLogger( Sys.class.getName() );

	private static final double NANOSECOND = 1e-9;

	public Random rg;
	public TaskScheduler scheduler;

	public AtomicLong uploadedBytes = new AtomicLong(1);
	public AtomicLong downloadedBytes = new AtomicLong(1);
	public String mainClass;

	private String datacenter = "*";

	public double currentTime() {
		return (System.nanoTime() - T0n) * NANOSECOND;
	}

	private double T0n = System.nanoTime();

	public long timeMillis() {
		return System.currentTimeMillis() - T0m;
	}

	private long T0m = System.currentTimeMillis();

	protected Sys() {
		Sys = this;
		
		StackTraceElement[] sta = Thread.currentThread().getStackTrace();
		mainClass = Thread.currentThread().getStackTrace()[sta.length-1].getClassName() + "@" + IP.localHostAddressString();
		initInstance();
	}

	protected void initInstance() {
		
		rg = new Random();
		scheduler = new TaskScheduler();
		scheduler.start();
		KryoCatadupa.init();
		KryoCRDTUtils.init();		
		
//		NetworkingImpl.Networking.setDefaultProvider( TransportProvider.NETTY_IO_TCP);
		NetworkingImpl.init();
	}

	public void setDatacenter(String datacenter) {
		this.datacenter = datacenter;
	}

	public String getDatacenter() {
		return datacenter;
	}

	public DHT getDHT_ClientStub() {
		return DHT_Node.getStub();
	}

	synchronized public static void init() {
		if (Sys == null) {
			new Sys();
		}
	}

	public static Sys Sys;
}
