package swift.crdt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.interfaces.CRDTUpdate;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;

import swift.crdt.SequenceVersioned.PosID;

public class SequenceVersioned<V> extends SortedSetVersioned<PosID<V>, SequenceVersioned<V>> {
	private static final long serialVersionUID = 1L;

	public SequenceVersioned() {
	}

	@Override
	protected TxnLocalCRDT<SequenceVersioned<V>> getTxnLocalCopyImpl(CausalityClock versionClock, TxnHandle txn) {
		final SequenceVersioned<V> creationState = isRegisteredInStore() ? null : new SequenceVersioned<V>();
		SequenceTxnLocal<V> localView = new SequenceTxnLocal<V>(id, txn, versionClock, creationState, getValue(versionClock));
		return localView;
	}

	@Override
	protected void execute(CRDTUpdate<SequenceVersioned<V>> op) {
		op.applyTo(this);
	}

	@Override
	public SequenceVersioned<V> copy() {
		SequenceVersioned<V> copy = new SequenceVersioned<V>();
		copyLoad(copy);
		copyBase(copy);
		return copy;
	}

	static class PosID<V> implements Comparable<PosID<V>> {

		V atom;
		SID id;
		Timestamp timestamp;

		// for kryo

		PosID() {
		}

		PosID(SID id, V atom) {
			this.id = id;
			this.atom = atom;
		}

		PosID(SID id, V atom, Timestamp ts) {
			this.id = id;
			this.atom = atom;
			this.timestamp = ts;
		}
		
		public SID getId() {
			return id;
		}

		public V getAtom() {
			return atom;
		}

		public boolean isDeleted() {
			return atom == null;
		}
		
		@Override
		public int compareTo(PosID<V> other) {
			int res = id.compareTo(other.id);
			return res != 0 ? res : timestamp.compareTo(other.timestamp);
		}

		public int hashCode() {
			return id.hashCode() ^ timestamp.hashCode() ;
		}

		public boolean equals(Object o) {
			PosID<V> other = (PosID<V>) o;
			return compareTo(other) == 0;
		}

		public PosID<V> deletedPosID() {
			return new PosID<V>(id, null, timestamp);
		}

		public String toString() {
			return String.format("<%s, %s>", id, atom);
		}

		public void setTimestamp(Timestamp ts) {
			this.timestamp = ts;
		}
	}
}

class SID implements Comparable<SID> {

	static final int INCREMENT = 1 << 4;
	static final Random rg = new Random(1L);
	static SID FIRST = new SID(new int[]{increment(0), SiteId.get()});

	int[] coords;

	// for kryo
	private SID() {
	}

	protected SID(int[] coords) {
		this.coords = coords;
	}

	public SID between(SID other) {
		int dims = Math.max(dims(), other.dims());
		int[] l = this.expandCoords(dims);
		int[] r = other.expandCoords(dims);
		return new SID(between(l, r));
	}

	// TODO deal with underoverflow of first coordinate...
	static public SID smallerThan(SID x) {
		int[] coords = Arrays.copyOf(x.coords, x.coords.length);
		coords[0] = decrement(x.coord(0));
		coords[1] = SiteId.get();
		return new SID(coords);
	}

	// TODO deal with overflow of first coordinate...
	static public SID greaterThan(SID x) {
		int[] coords = Arrays.copyOf(x.coords, x.coords.length);
		coords[0] = increment(x.coord(0));
		coords[1] = SiteId.get();
		return new SID(coords);
	}

	@Override
	public int compareTo(SID other) {
		int dims = Math.max(dims(), other.dims());
		for (int i = 0; i < dims; i++) {
			int signum = coord(i) - other.coord(i);
			if (signum != 0)
				return signum;
		}
		return 0;
	}

	/*
	 * returns an expanded coordinate vector, filling with zeros the extra
	 * dimensions, except the last one, which is filled with the local site's
	 * dis-ambiguator.
	 */
	final private int[] expandCoords(int dims) {
		if (dims == dims())
			return coords;

		int[] res = Arrays.copyOf(coords, dims);
		res[dims - 1] = SiteId.get();
		return res;
	}

	/*
	 * l and r have the same number of dimensions...
	 */
	static private int[] between(int[] l, int[] r) {

		assert l.length == r.length;

		int dims = l.length;

		int[] res = new int[dims];

		res[dims - 1] = SiteId.get();

		for (int i = 0; i < dims - 1; i++) {
			res[i] = (r[i] + l[i]) >> 1;
			// int quarter = (res[i] - l[i]) >> 2;
			// if (quarter > 2)
			// res[i] = res[i] - quarter + rg.nextInt(2*quarter);
		}

		for (int i = 0; i < dims - 1; i++)
			if (res[i] != l[i])
				return res;

		// we got a collision, increase #dims by 2.
		res = Arrays.copyOf(res, dims + 2);
		res[dims] = 1024; // Integer.MAX_VALUE >> 1 ;
		res[dims + 1] = SiteId.get();

		return res;
	}

	public int hashCode() {
		int res = 0;

		for (int i : coords)
			res ^= i;

		return res;
	}

	public boolean equals(Object other) {
		return (other instanceof SID) && compareTo((SID) other) == 0;
	}

	private int coord(int d) {
		return d < coords.length ? coords[d] : 0;
	}

	public String toString() {
		List<Integer> tmp = new ArrayList<Integer>();
		for (int i : coords)
			tmp.add(i);

		return String.format("%s", tmp);
	}

	static private int increment(int base) {
		final int jitter = (INCREMENT >> 2) + rg.nextInt(INCREMENT >> 1);
		return base + jitter;
	}

	static private int decrement(int base) {
		final int jitter = (INCREMENT >> 2) + rg.nextInt(INCREMENT >> 1);
		return base - jitter;
	}

	final private int dims() {
		return coords.length;
	}
}

// TODO Needs to initialize SiteId from somewhere else...
class SiteId {

	private static final AtomicInteger uniqueId = new AtomicInteger(-1);

	static final ThreadLocal<Integer> uniqueNum = new ThreadLocal<Integer>() {
		@Override
		protected Integer initialValue() {
			return uniqueId.getAndIncrement();
		}
	};

	public static Integer get() {
		return uniqueId.get();
	}

}