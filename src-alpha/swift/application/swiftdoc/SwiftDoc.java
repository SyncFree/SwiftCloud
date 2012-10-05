package swift.application.swiftdoc;

import java.util.Random;

import swift.client.AbstractObjectUpdatesListener;
import swift.client.SwiftImpl;
import swift.crdt.CRDTIdentifier;
import swift.crdt.SequenceTxnLocal;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;
import sys.Sys;
import sys.utils.Threading;

/**
 * Local setup/test with one server and two clients.
 * 
 * @author annettebieniusa, smduarte
 * 
 */
public class SwiftDoc {
	private static String sequencerName = "localhost";
	private static String dcName = "localhost";
	static int iterations = 10;
	static IsolationLevel isolationLevel = IsolationLevel.REPEATABLE_READS;
	static CachePolicy cachePolicy = CachePolicy.CACHED;
	static boolean notifications = true;
	static CRDTIdentifier j = new CRDTIdentifier("doc", "2");

	public static void main(String[] args) {
		System.out.println("DocLatencyTest start!");
		// start sequencer server
		DCSequencerServer.main(new String[]{"-name", sequencerName});

		// start DC server
		DCServer.main(new String[]{dcName});

		Thread client1 = new Thread("client1") {
			public void run() {
				Sys.init();
				SwiftImpl clientServer = SwiftImpl.newInstance(dcName, DCConstants.SURROGATE_PORT);
				runClient1(clientServer);
			}
		};
		client1.start();

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		Thread client2 = new Thread("client2") {
			public void run() {
				Sys.init();
				SwiftImpl clientServer = SwiftImpl.newInstance(dcName, DCConstants.SURROGATE_PORT);
				runClient2(clientServer);
			}
		};
		client2.start();
	}

	static void runClient1(SwiftImpl swift) {
		client1CodeNotifications(swift);
	}

	static void runClient2(SwiftImpl swift) {
		Threading.sleep(5000);
		client2CodeNotifications(swift);
	}

	protected static void client1CodeNotifications(SwiftImpl swift) {
		try {
			String L = "ABCDEFGHIJKLMNOPQRSTUVXYZ";
			{
				for (int i = 0; i < 10; i++) {
					TxnHandle handle = swift.beginTxn(isolationLevel, cachePolicy, false);
					SequenceTxnLocal<String> doc = handle.get(j, i == 0, swift.crdt.SequenceVersioned.class, null);
					doc.insertAt(0, "" + L.charAt(i));
					System.err.println("Writer get():" + doc.getValue());
					handle.commit();
					Threading.sleep(2000);
				}
			}
			{
				for (int i = 0; i < 10; i++) {
					TxnHandle handle = swift.beginTxn(isolationLevel, cachePolicy, true);
					SequenceTxnLocal<String> doc = handle.get(j, i == 0, swift.crdt.SequenceVersioned.class, null);
					doc.removeAt(0);
					System.err.println("Writer get():" + doc.getValue());
					handle.commit();
					Threading.sleep(2000);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	protected static void client2CodeNotifications(final SwiftImpl swift) {
		try {
			// Need cache policy MOST_RECENT for first read
			
			while (true) {
				final Object mon = new Object();
				final TxnHandle handle = swift.beginTxn(isolationLevel, CachePolicy.CACHED, false);
				SequenceTxnLocal<String> doc = handle.get(j, true, swift.crdt.SequenceVersioned.class, new AbstractObjectUpdatesListener() {					
					public void onObjectUpdate(TxnHandle txn, CRDTIdentifier id, TxnLocalCRDT<?> previousValue) {
						Threading.synchronizedNotifyAllOn(mon);
						System.err.println( "previous:" + previousValue.getValue() );
					}
				});
				Threading.synchronizedWaitOn(mon, 5000);
				System.err.println( "Triggered Reader get():" + doc.getValue() );
				handle.commit();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
