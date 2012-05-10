package sys.utils;

import java.util.HashMap;
import java.util.Map;

/**
 * A collection of convenience methods for dealing with threads.
 * 
 * @author Sérgio Duarte (smd@fct.unl.pt)
 * 
 */
public class Threading {

	protected Threading() {
	}

	static public Thread newThread(boolean daemon, Runnable r) {
		Thread res = new Thread(r);
		res.setDaemon(daemon);
		return res;
	}

	static public Thread newThread(Runnable r, boolean daemon) {
		Thread res = new Thread(r);
		res.setDaemon(daemon);
		return res;
	}

	static public void sleep(int ms) {
		try {
			Thread.sleep(ms);
		} catch (InterruptedException x) {
			x.printStackTrace();
		}
	}

	static public void sleep(int ms, int ns) {
		try {
			Thread.sleep(ms, ns);
		} catch (InterruptedException x) {
			x.printStackTrace();
		}
	}

	static public void waitOn(Object o) {
		try {
			o.wait();
		} catch (InterruptedException x) {
			x.printStackTrace();
		}
	}

	static public void waitOn(Object o, int ms) {
		try {
			if( ms > 0 )
				o.wait(ms);
		} catch (InterruptedException x) {
			x.printStackTrace();
		}
	}

	static public void notifyOn(Object o) {
		o.notify();
	}

	static public void notifyAllOn(Object o) {
		o.notifyAll();
	}

	static public void synchronizedWaitOn(Object o) {
		synchronized (o) {
			try {
				o.wait();
			} catch (InterruptedException x) {
				x.printStackTrace();
			}
		}
	}

	static public void synchronizedWaitOn(Object o, int ms) {
	    synchronized (o) {
			try {
				o.wait(ms);
			} catch (InterruptedException x) {
				x.printStackTrace();
			}
		}
	}

	static public void synchronizedNotifyOn(Object o) {
		synchronized (o) {
			o.notify();
		}
	}

	static public void synchronizedNotifyAllOn(Object o) {
		synchronized (o) {
			o.notifyAll();
		}
	}

	/**
	 * Returns an object "value" (rather than any instance) to serve as a
	 * monitor. Leaks memory...
	 * 
	 * @param o
	 * @return
	 */
	synchronized static public Object lockFor(Object o) {

		Object res = locks.get(o);
		if (res == null) {
			res = o;
			locks.put(res, res);
		}
		return res;
	}

	static Map<Object, Object> locks = new HashMap<Object, Object>();
}
