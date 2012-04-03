package sys;

import java.util.Random;

import sys.dht.DHT_Node;
import sys.dht.api.DHT;
import sys.net.impl.NetworkingImpl;
import sys.scheduler.TaskScheduler;

public class Sys {

    private static final double NANOSECOND = 1e-9;
    
    public static Sys Sys;

    public Random rg;
    public TaskScheduler scheduler;

    public long uploadedBytes = 1;
    public long downloadedBytes = 1;

    private double T0;
    public double currentTime() {
        return (System.nanoTime() - T0) * NANOSECOND;
    }

    protected Sys() {
    }

    protected void initInstance() {
        rg = new Random(1L);
        T0 = System.nanoTime();
        scheduler = new TaskScheduler();
        scheduler.start();
        NetworkingImpl.init();
    }

    public DHT getDHT_ClientStub() {
        return DHT_Node.getStub();
    }
    
    public synchronized static void init() {
        if( Sys != null)
            return;
        Sys = new Sys();
        Sys.initInstance();
    }

}
