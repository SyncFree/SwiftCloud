package sys.stats;

import java.util.Map;
import java.util.HashMap;

import sys.net.api.Endpoint;
import sys.net.impl.rpc.RpcPacket;
import sys.scheduler.PeriodicTask;
import sys.utils.XmlExternalizable;

import static sys.Sys.*;

public class RpcStats extends XmlExternalizable {
	private static double STATS_BIN_SIZE = 60.0;
	
	double T0 = Sys.currentTime();
	
	Map<Endpoint, BinnedTally> rpcRTT = new HashMap<Endpoint, BinnedTally>();
	Map<String, BinnedTally> inMsgTraffic = new HashMap<String, BinnedTally>();
	Map<String, BinnedTally> outMsgTraffic = new HashMap<String, BinnedTally>();
	Map<Class<?>, BinnedTally> rpcExecTime = new HashMap<Class<?>, BinnedTally>();

 	protected RpcStats() {
	}

	synchronized public void logSentRpcPacket(RpcPacket pkt, Endpoint dst) {
		String type = pkt.getPayload().getClass().getName();
		valueFor( outMsgTraffic, type, true).tally( Sys.currentTime(), pkt.getSize() ) ;
	}

	synchronized public void logReceivedRpcPacket(RpcPacket pkt, Endpoint src) {
		String type = pkt.getPayload().getClass().getName();
		valueFor( inMsgTraffic, type, true).tally( Sys.currentTime(), pkt.getSize() ) ;
	}

	synchronized public void logRpcExecTime(Class<?> cl, double time) {
		valueFor( rpcExecTime, cl, true).tally( Sys.currentTime(), time ) ;
	}

	synchronized public void logRpcRTT(Endpoint dst, double rtt) {
		valueFor( rpcRTT, dst, true).tally( Sys.currentTime(), rtt ) ;
	}

	private synchronized <K> BinnedTally valueFor( Map<K, BinnedTally> map, K key, boolean create ) {
		BinnedTally res = map.get(key);
		if( res == null && create ) {
			map.put( key, res = new BinnedTally(STATS_BIN_SIZE, "") ) ;
		}
		return res;
	}
	
	static {
		RpcStats = new RpcStats();

		new PeriodicTask(0, 30) {
			public void run() {
				synchronized (RpcStats) {
					try {
						RpcStats.saveXmlTo("./" + Sys.mainClass + "-stats.xml");
					} catch (Exception x) {
						x.printStackTrace();
					}
				}
			}
		};
	}
	
	public static RpcStats RpcStats;
}
