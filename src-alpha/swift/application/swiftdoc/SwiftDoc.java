package swift.application.swiftdoc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

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
import sys.Sys;
import sys.utils.Threading;

/**
 * 
 * @author smduarte, annettebieniusa
 * 
 */
public class SwiftDoc {
	private static String sequencerName = "localhost";
	private static String dcName = "localhost";
	static int iterations = 10;
	static IsolationLevel isolationLevel = IsolationLevel.REPEATABLE_READS;
	static CachePolicy cachePolicy = CachePolicy.CACHED;
	static boolean notifications = true;
	static CRDTIdentifier j1 = new CRDTIdentifier("doc", "1");
	static CRDTIdentifier j2 = new CRDTIdentifier("doc", "2");

	public static void main(String[] args) {
		System.out.println("SwiftDoc start!");
		// start sequencer server
		DCSequencerServer.main(new String[]{"-name", sequencerName});

		// start DC server
		DCServer.main(new String[]{dcName});

		Threading.newThread("client2", true, new Runnable() {
			public void run() {
				Sys.init();
				SwiftImpl swift1 = SwiftImpl.newInstance(dcName, DCConstants.SURROGATE_PORT);
				SwiftImpl swift2 = SwiftImpl.newInstance(dcName, DCConstants.SURROGATE_PORT);
				runClient1(swift1, swift2);
			}
		}).start();

		Threading.sleep(1000);

		Threading.newThread("client2", true, new Runnable() {
			public void run() {
				Sys.init();
				SwiftImpl swift1 = SwiftImpl.newInstance(dcName, DCConstants.SURROGATE_PORT);
				SwiftImpl swift2 = SwiftImpl.newInstance(dcName, DCConstants.SURROGATE_PORT);
				runClient2(swift1, swift2);
			}
		}).start();
	}

	static void runClient1(SwiftImpl swift1, SwiftImpl swift2) {
		client1code(swift1, swift2);
	}

	static void runClient2(SwiftImpl swift1, SwiftImpl swift2) {
		client2code(swift1, swift2);
	}

	static void client1code(final SwiftImpl swift1, final SwiftImpl swift2) {
		try {
			final AtomicBoolean done = new AtomicBoolean(false);
			final Map<Long, TextLine> samples = new HashMap<Long, TextLine>();

			
			Threading.newThread(true, new Runnable() {
				public void run() {
					try {
						for (int k = 0; !done.get(); k++) {
							final Object barrier = new Object();
							final TxnHandle handle = swift2.beginTxn(isolationLevel, k == 0 ? CachePolicy.MOST_RECENT : CachePolicy.CACHED, true);
							SequenceTxnLocal<TextLine> doc = handle.get(j2, true, swift.crdt.SequenceVersioned.class, new AbstractObjectUpdatesListener() {
								public void onObjectUpdate(TxnHandle txn, CRDTIdentifier id, TxnLocalCRDT<?> previousValue) {
									Threading.synchronizedNotifyAllOn(barrier);
		                            //System.err.println("Triggered Reader get():" + previousValue.getValue());
								}
							});
							Threading.synchronizedWaitOn(barrier, 5000);
							for (TextLine i : doc.getValue()) {
								if (!samples.containsKey(i.serial())) {
									samples.put(i.serial(), i);
								}
							}
							handle.commit();
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}).start();

			SwiftDocPatchReplay<TextLine> player = new SwiftDocPatchReplay<TextLine>();

			player.parseFiles(new SwiftDocOps<TextLine>() {
				TxnHandle handle = null;
				SequenceTxnLocal<TextLine> doc = null;
				@Override
				public void begin() {
					try {
						handle = swift1.beginTxn(isolationLevel, cachePolicy, false);
						doc = handle.get(j1, true, swift.crdt.SequenceVersioned.class, null);
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
					Threading.sleep(1000);
				}

				@Override
				public TextLine gen(String s) {
					return new TextLine(s);
				}
			} );
			done.set(true);


			for (TextLine i : new ArrayList<TextLine>(samples.values()))
				System.out.printf("%s\n", i.latency());
			
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	static void client2code(final SwiftImpl swift1, final SwiftImpl swift2) {
		try {
			final Set<Long> serials = new HashSet<Long>();

			for (int k = 0;; k++) {
				final Object barrier = new Object();
				final TxnHandle handle = swift1.beginTxn(isolationLevel, k == 0 ? CachePolicy.MOST_RECENT : CachePolicy.CACHED, true);
				SequenceTxnLocal<TextLine> doc = handle.get(j1, true, swift.crdt.SequenceVersioned.class, new AbstractObjectUpdatesListener() {
					public void onObjectUpdate(TxnHandle txn, CRDTIdentifier id, TxnLocalCRDT<?> previousValue) {
						Threading.synchronizedNotifyAllOn(barrier);
						//System.err.println("previous:" + previousValue.getValue());
					}
				});

				// Wait for the notification, before reading the new value of
				// the sequence...
				Threading.synchronizedWaitOn(barrier, 5000);
				// System.err.println("Triggered Reader get():" +
				// doc.getValue());

				// Determine the new atoms this update brought...
				final Collection<TextLine> newAtoms = new ArrayList<TextLine>();
				for (TextLine i : doc.getValue()) {
					if (serials.add(i.serial())) {
						newAtoms.add(i);
					}
				}
				handle.commit();

				// Write the atoms into the other sequence to measure RTT...
				Threading.newThread(true, new Runnable() {
					public void run() {
						synchronized (serials) {//wait for the previous transaction to complete...
							try {
                                final TxnHandle handle = swift2.beginTxn(isolationLevel, CachePolicy.CACHED, false);
								SequenceTxnLocal<TextLine> doc2 = handle.get(j2, true, swift.crdt.SequenceVersioned.class, null);
								for (TextLine i : newAtoms)
									doc2.insertAt(doc2.size(), i);
								handle.commit();
							} catch (Exception x) {
								x.printStackTrace();
							}
						}
					}
				}).start();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
