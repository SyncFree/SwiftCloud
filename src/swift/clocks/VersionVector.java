package swift.clocks;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import swift.exceptions.IncompatibleTypeException;

/**
 * Class to represent common version vectors.
 *
 * @author nmp
 */
// TODO: provide custom serializer or Kryo-lize the class
public class VersionVector implements CausalityClock {

    private static final long serialVersionUID = 1L;
    protected Map<String, Long> vv;

    protected VersionVector() {
        vv = new TreeMap<String, Long>();
    }

    protected VersionVector(VersionVector v) {
        vv = new TreeMap<String, Long>(v.vv);
    }

    /**
     * Records the given event. Assume the timestamp can be recorded in the
     * given version vector.
     *
     * @param ec Event clock.
     * @return Returns false if the object was already recorded.
     */
    public boolean record(Timestamp cc) {
        if (cc.getCounter() == Timestamp.MIN_VALUE) {
            return false;
        }
        Long i = vv.get(cc.getIdentifier());
        vv.put(cc.getIdentifier(), cc.getCounter());
        return i == null || i < cc.getCounter();
    }

    @Override
    public void recordAllUntil(Timestamp timestamp) {
        record(timestamp);
    }

    /**
     * Returns the most recent event for a given site. <br>
     *
     * @param siteid Site identifier.
     * @return Returns an event clock.
     */
    public Timestamp getLatest(String siteid) {
        Long i = vv.get(siteid);
        if (i == null) {
            return new Timestamp(siteid, Timestamp.MIN_VALUE);
        } else {
            return new Timestamp(siteid, i);
        }
    }

    /**
     * Returns the most recent event for a given site. <br>
     *
     * @param siteid Site identifier.
     * @return Returns an event clock.
     */
    public long getLatestCounter(String siteid) {
        Long i = vv.get(siteid);
        if (i == null) {
            return Timestamp.MIN_VALUE;
        } else {
            return i;
        }
    }

    @Override
    public boolean hasEventFrom(String siteid) {
        return getLatestCounter(siteid) != Timestamp.MIN_VALUE;
    }

    /**
     * Compares the elements in the version vector.
     *
     * @param c Clock to compare to
     * @return Returns one of the following:<br> CMP_EQUALS : if clocks are
     * equal; <br> CMP_DOMINATES : if this clock dominates the given c clock;
     * <br> CMP_ISDOMINATED : if this clock is dominated by the given c clock;
     * <br> CMP_CONCUREENT : if this clock and the given c clock are concurrent;
     * <br>
     * @throws IncompatibleTypeException Case comparison cannot be made
     */
    protected CMP_CLOCK compareToVV(VersionVector cc) {
        Iterator<Entry<String, Long>> itThis = vv.entrySet().iterator();
        Iterator<Entry<String, Long>> itOther = cc.vv.entrySet().iterator();

        for (;;) {
            if (itThis.hasNext() && !itOther.hasNext()) {
                return CMP_CLOCK.CMP_DOMINATES;
            }
            if (!itThis.hasNext() && itOther.hasNext()) {
                return CMP_CLOCK.CMP_ISDOMINATED;
            }
            if (!itThis.hasNext() && !itOther.hasNext()) {
                return CMP_CLOCK.CMP_EQUALS;
            }
            Entry<String, Long> itThisOne = itThis.hasNext() ? itThis.next() : null;
            Entry<String, Long> itOtherOne = itOther.hasNext() ? itOther.next() : null;
            int c = itThisOne.getKey().compareTo(itOtherOne.getKey());
            if (c == 0) {
                int cv = Long.signum(itThisOne.getValue() - itOtherOne.getValue());
                if (cv < 0) {
                    return CMP_CLOCK.CMP_ISDOMINATED;
                } else if (cv > 0) {
                    return CMP_CLOCK.CMP_DOMINATES;
                }
                continue;
            }
            if (c < 0) {
                return CMP_CLOCK.CMP_ISDOMINATED;
            } else {
                return CMP_CLOCK.CMP_DOMINATES;
            }
        }
    }

    /**
     * Compares two causality clock.
     *
     * @param c Clock to compare to
     * @return Returns one of the following:<br> CMP_EQUALS : if clocks are
     * equal; <br> CMP_DOMINATES : if this clock dominates the given c clock;
     * <br> CMP_ISDOMINATED : if this clock is dominated by the given c clock;
     * <br> CMP_CONCUREENT : if this clock and the given c clock are concurrent;
     * <br>
     * @throws IncompatibleTypeException Case comparison cannot be made
     */
    public CMP_CLOCK compareTo(CausalityClock cc) {
        if (VersionVector.class.equals(cc.getClass())) {
            return compareToVV((VersionVector) cc);
        } else {
            CMP_CLOCK c = cc.compareTo(this);
            if (c == CMP_CLOCK.CMP_CONCURRENT) {
                return c;
            } else if (c == CMP_CLOCK.CMP_EQUALS) {
                return c;
            } else if (c == CMP_CLOCK.CMP_DOMINATES) {
                return CMP_CLOCK.CMP_ISDOMINATED;
            } else {
                return CMP_CLOCK.CMP_DOMINATES;
            }
        }
    }

    /**
     * Checks if a given event clock is reflected in this clock
     *
     * @param c Event clock.
     * @return Returns true if the given event clock is included in this
     * causality clock.
     * @throws IncompatibleTypeException
     */
    @Override
    public boolean includes(Timestamp cc) {
        Long i = vv.get(cc.getIdentifier());
        return i != null && cc.getCounter() <= i;
    }

    /**
     * Merge this clock with the given c clock. TODO: This is inefficient.
     *
     * @param c Clock to merge to
     * @return Returns one of the following, based on the initial value of
     * clocks:<br> CMP_EQUALS : if clocks were equal; <br> CMP_DOMINATES : if
     * this clock dominated the given c clock; <br> CMP_ISDOMINATED : if this
     * clock was dominated by the given c clock; <br> CMP_CONCUREENT : if this
     * clock and the given c clock were concurrent; <br>
     */
    public CMP_CLOCK mergeVV(VersionVector cc) {
        boolean lessThan = false;
        boolean greaterThan = false;
        Iterator<Entry<String, Long>> it = cc.vv.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Long> e = it.next();
            Long i = vv.get(e.getKey());
            if (i == null) {
                lessThan = true;
                vv.put(e.getKey(), e.getValue());
            } else {
                long iOther = e.getValue();
                long iThis = i;
                if (iThis < iOther) {
                    lessThan = true;
                    vv.put(e.getKey(), iOther);
                } else if (iThis > iOther) {
                    greaterThan = true;
                }
            }
        }
        it = vv.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Long> e = it.next();
            Long i = cc.vv.get(e.getKey());
            if (i == null) {
                greaterThan = true;
                break;
            }
        }
        if (greaterThan && lessThan) {
            return CMP_CLOCK.CMP_CONCURRENT;
        }
        if (greaterThan) {
            return CMP_CLOCK.CMP_DOMINATES;
        }
        if (lessThan) {
            return CMP_CLOCK.CMP_ISDOMINATED;
        }
        return CMP_CLOCK.CMP_EQUALS;

    }

    /**
     * Merge this clock with the given c clock.
     *
     * @param c Clock to merge to
     * @return Returns one of the following, based on the initial value of
     * clocks:<br> CMP_EQUALS : if clocks were equal; <br> CMP_DOMINATES : if
     * this clock dominated the given c clock; <br> CMP_ISDOMINATED : if this
     * clock was dominated by the given c clock; <br> CMP_CONCUREENT : if this
     * clock and the given c clock were concurrent; <br>
     * @throws IncompatibleTypeException Case comparison cannot be made
     */
    public CMP_CLOCK merge(CausalityClock cc) {
        // if ( ! VersionVector.class.equals(cc.getClass())) {
        // throw new IncompatibleTypeException();
        // }
        return mergeVV((VersionVector) cc);
    }

    /**
     * Create a copy of this causality clock.
     */
    public CausalityClock clone() {
        return new VersionVector(this);
    }

    @Override
    public String toString() {
        StringBuffer buf = new StringBuffer();
        buf.append("[");
        Iterator<Entry<String, Long>> it = vv.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Long> e = it.next();
            buf.append(e.getKey() + ":" + e.getValue());
            if (it.hasNext()) {
                buf.append(",");
            }
        }
        buf.append("]");
        return buf.toString();
    }

    @Override
    public boolean hasExceptions() {
        return false;
    }

    @Override
    public void drop(String siteId) {
        vv.remove(siteId);
    }

    @Override
    public void drop(Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object copy() {
        return new VersionVector(this);
    }
}
