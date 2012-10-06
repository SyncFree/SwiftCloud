package swift.application.swiftdoc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
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
		System.out.println("SwiftDoc start!");
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
		client1CodeNotifications_PerPatch(swift);
	}

	static void runClient2(SwiftImpl swift) {
		Threading.sleep(1000);
		client2CodeNotifications(swift);
	}

	protected static void client1CodeNotifications_1op(final SwiftImpl swift) {
		try {
			TxnHandle handle = swift.beginTxn(isolationLevel, cachePolicy, false);
			SequenceTxnLocal<TextLine> doc = handle.get(j, true, swift.crdt.SequenceVersioned.class, null);
			//System.out.printf(" - -->(%s, %s)  %s\n", pos, res, doc.getValue() );
			handle.commit();

			
			SwiftDocPatchReplay<TextLine> player = new SwiftDocPatchReplay<TextLine>();

			player.parseFiles(new SwiftDocOps<TextLine>() {

				public TextLine remove(int pos) {
					TextLine res = null;
					try {
						TxnHandle handle = swift.beginTxn(isolationLevel, cachePolicy, false);
						SequenceTxnLocal<TextLine> doc = handle.get(j, false, swift.crdt.SequenceVersioned.class, null);
						res = doc.removeAt(pos);
						//System.out.printf(" - -->(%s, %s)  %s\n", pos, res, doc.getValue() );
						handle.commit();
					} catch (Exception x) {
						x.printStackTrace();
						System.exit(0);
					}
					return res;
				}

				@Override
				public TextLine get(int pos) {
					TextLine res = null;
					try {
						TxnHandle handle = swift.beginTxn(isolationLevel, cachePolicy, false);
						SequenceTxnLocal<TextLine> doc = handle.get(j, false, swift.crdt.SequenceVersioned.class, null);
						res = doc.getValue().get(pos);
						//System.out.printf(" ? -->(%s, %s)  %s\n", pos, res, doc.getValue() );
						handle.commit();
					} catch (Exception x) {
						x.printStackTrace();
						System.exit(0);
					}
					return res;
				}

				@Override
				public void add(int pos, TextLine atom) {
					try {
						TxnHandle handle = swift.beginTxn(isolationLevel, cachePolicy, false);
						SequenceTxnLocal<TextLine> doc = handle.get(j, false, swift.crdt.SequenceVersioned.class, null);
						//System.out.printf(" + -->(%s, %s)  %s\n", pos, atom, doc.getValue() );
						doc.insertAt(pos, atom);
						handle.commit();
					} catch (Exception x) {
						x.printStackTrace();
						System.exit(0);
					}
				}

				@Override
				public int size() {
					int res = 0;
					try {
						TxnHandle handle = swift.beginTxn(isolationLevel, cachePolicy, false);
						SequenceTxnLocal<TextLine> doc = handle.get(j, false, swift.crdt.SequenceVersioned.class, null);
						res = doc.size();
						handle.commit();
					} catch (Exception x) {
						x.printStackTrace();
					}
					return res;
				}

				@Override
				public void begin() {
				}

				@Override
				public void commit() {
				}

				@Override
				public TextLine gen(String s) {
					return new TextLine(s);
				}
			});

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	protected static void client1CodeNotifications_PerPatch(final SwiftImpl swift) {
		try {
			TxnHandle handle = swift.beginTxn(isolationLevel, cachePolicy, false);
			SequenceTxnLocal<String> doc = handle.get(j, true, swift.crdt.SequenceVersioned.class, null);
			//System.out.printf(" - -->(%s, %s)  %s\n", pos, res, doc.getValue() );
			handle.commit();

			
			SwiftDocPatchReplay<TextLine> player = new SwiftDocPatchReplay<TextLine>();

			player.parseFiles(new SwiftDocOps<TextLine>() {

				TxnHandle handle = null;
				SequenceTxnLocal<TextLine> doc = null;
				@Override
				public void begin() {
					try {
						handle = swift.beginTxn(isolationLevel, cachePolicy, false);
						doc = handle.get(j, false, swift.crdt.SequenceVersioned.class, null);
					} catch (Throwable e) {
						e.printStackTrace();
						System.exit(0);
					}
				}
				
				public TextLine remove(int pos) {
					return doc.removeAt(pos);
				}

				@Override
				public TextLine get(int pos) {
					return doc.getValue().get(pos);
				}

				@Override
				public void add(int pos, TextLine atom) {
					doc.insertAt(pos, atom);
				}

				@Override
				public int size() {
					return doc.size();
				}

				@Override
				public void commit() {
					handle.commit();
					handle = null;
				}

				@Override
				public TextLine gen(String s) {
					return new TextLine(s);
				}
			});

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
	protected static void client2CodeNotifications(final SwiftImpl swift) {
		try {
			// Need cache policy MOST_RECENT for first read
			final Map<Long, Long> latency = new HashMap<Long,Long>();
			
			while (true) {
				final Object mon = new Object();
				final TxnHandle handle = swift.beginTxn(isolationLevel, CachePolicy.CACHED, true);
				SequenceTxnLocal<TextLine> doc = handle.get(j, true, swift.crdt.SequenceVersioned.class, new AbstractObjectUpdatesListener() {
					public void onObjectUpdate(TxnHandle txn, CRDTIdentifier id, TxnLocalCRDT<?> previousValue) {
						Threading.synchronizedNotifyAllOn(mon);
						//System.err.println("previous:" + previousValue.getValue());
					}
				});
				Threading.synchronizedWaitOn(mon, 50);
				//System.err.println("Triggered Reader get():" + doc.getValue());
				final Map<Long, Long> tmp = new HashMap<Long,Long>();
				
				for( TextLine i : doc.getValue() ) {
					if( ! latency.containsKey( i.serial() ) ) {
						tmp.put( i.serial(), i.latency() ) ;
					}
				}
				System.out.println("new atoms:" + tmp.values() );
				handle.commit();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
