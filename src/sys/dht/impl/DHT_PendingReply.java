package sys.dht.impl;

import static sys.Sys.Sys;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import sys.dht.api.DHT;
import sys.scheduler.PeriodicTask;

class DHT_PendingReply {
	final static double LEASE_DURATION = 300.0;

	final DHT.ReplyHandler handler;
	final long handlerId = g_handlers++;
	final double timestamp = Sys.currentTime();

	// need to gc handlers that do not get a reply. Quick/dirty solution is to
	// delete those old
	// enough...
	static Map<Long, DHT_PendingReply> handlers = new HashMap<Long, DHT_PendingReply>();

	public DHT_PendingReply(DHT.ReplyHandler handler) {
		this.handler = handler;
		synchronized (handlers) {
			handlers.put(handlerId, this);
		}
	}

	static DHT_PendingReply getHandler(long handlerId) {
		synchronized (handlers) {
			return handlers.remove(handlerId);
		}
	}

	static {
		new PeriodicTask(0, LEASE_DURATION / 2) {
			@Override
			public void run() {
				double now = Sys.currentTime();
				synchronized (handlers) {
					for (Iterator<DHT_PendingReply> it = handlers.values().iterator(); it.hasNext();) {
						if (now - it.next().timestamp > LEASE_DURATION) {
							it.remove();
						}
					}
				}
			}
		};
	}
	static long g_handlers;
}
