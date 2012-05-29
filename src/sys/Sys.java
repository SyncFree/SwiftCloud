package sys;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import swift.utils.KryoCRDTUtils;
import sys.dht.DHT_Node;
import sys.dht.api.DHT;
import sys.dht.catadupa.KryoCatadupa;
import sys.net.impl.NetworkingImpl;
import sys.scheduler.TaskScheduler;

public class Sys {

	private static final double NANOSECOND = 1e-9;

	public Random rg;
	public TaskScheduler scheduler;

	public AtomicLong uploadedBytes = new AtomicLong(1);
	public AtomicLong downloadedBytes = new AtomicLong(1);

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
		initInstance();
	}

	protected void initInstance() {
		rg = new Random(1L);
		scheduler = new TaskScheduler();
		scheduler.start();
		NetworkingImpl.init();
		KryoCatadupa.init();
		KryoCRDTUtils.init();
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
			Sys = new Sys();
		}
	}

	public static Sys Sys;
}
