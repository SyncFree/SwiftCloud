package sys.dht.catadupa.crdts;

import java.util.Collection;
import java.util.Set;

import sys.dht.catadupa.crdts.time.Timestamp;

public abstract class AbstractORSet<T> implements Set<T> {

	protected CRDTRuntime rt = null;

	public AbstractORSet<T> setUpdatesRecorder(CRDTRuntime rt) {
		this.rt = rt;
		return this;
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		boolean changed = false;
		for (T i : c)
			changed |= add(i);
		return changed;
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		for (Object i : c)
			if (!contains(i))
				return false;
		return true;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		boolean changed = false;
		for (Object i : c)
			changed |= remove(i);

		return changed;
	}

	@Override
	public Object[] toArray() {
		return toArray(new Object[size()]);
	}

	abstract public boolean add(T t, Timestamp ts);

}
