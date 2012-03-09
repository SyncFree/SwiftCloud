package swift.clocks;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import swift.exceptions.IncompatibleTypeException;

/**
 * Class to represent a dotted version vector.<br>
 * A dotted version vector extends a common version vector with a timestamp that
 * can be decoupled from the normal sequence.
 * 
 * @author nmp
 */
// TODO: provide custom serializer or Kryo-lize the class
public class DottedVersionVector implements CausalityClock {
    protected Timestamp ts;
    protected Map<String, Long> vv;

    protected DottedVersionVector(Timestamp ts) {
        this.ts = ts;
        vv = new TreeMap<String, Long>();
    }

    protected DottedVersionVector(DottedVersionVector v) {
        vv = new TreeMap<String, Long>(v.vv);
        if (v.ts != null) {
            ts = v.ts.clone();
        }
    }

    protected void normalize() {
        if (ts == null) {
            return;
        }
        vv.put(ts.getIdentifier(), ts.getCounter());
        ts = null;
    }

    /**
     * Records the next event. <br>
     * IMPORTANT NOTE: Assumes no holes in previous event history.
     * 
     * @param ec
     *            Event clock.
     * @return Returns true if the given event clock is included in this
     *         causality clock.
     * @throws IncompatibleTypeException
     */
    public boolean record(Timestamp c) {
        if (c.equals(this.ts)) {
            return false;
        }
        Long i = vv.get(c.getIdentifier());
        if (i != null && i >= c.getCounter()) {
            return false;
        }
        normalize();
        ts = c;
        return true;
    }

    /**
     * Returns true if vv1 <= vv2, i.e., vv1 is equal or dominated by vv2
     * 
     * @param vv1
     * @param vv1
     * @return
     */
    protected boolean isEqualOrDominatedLessSite(Map<String, Long> vv1, Map<String, Long> vv2, String siteid) {
        Iterator<Entry<String, Long>> it = vv1.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Long> e = it.next();
            if (siteid.equals(e.getKey())) {
                continue;
            }
            Long i = vv2.get(e.getKey());
            if (i == null) {
                return false;
            } else {
                if (i < e.getValue()) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Compares the elements in the version vector.
     * 
     * @param c
     *            Clock to compare to
     * @return Returns one of the following:<br>
     *         CMP_EQUALS : if clocks are equal; <br>
     *         CMP_DOMINATES : if this clock dominates the given c clock; <br>
     *         CMP_ISDOMINATED : if this clock is dominated by the given c
     *         clock; <br>
     *         CMP_CONCUREENT : if this clock and the given c clock are
     *         concurrent; <br>
     * @throws IncompatibleTypeException
     *             Case comparison cannot be made
     */
    protected CMP_CLOCK compareToVV(DottedVersionVector cc) {
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
     * @param c
     *            Clock to comapre to
     * @return Returns one of the following:<br>
     *         CMP_EQUALS : if clocks are equal; <br>
     *         CMP_DOMINATES : if this clock dominates the given c clock; <br>
     *         CMP_ISDOMINATED : if this clock is dominated by the given c
     *         clock; <br>
     *         CMP_CONCUREENT : if this clock and the given c clock are
     *         concurrent; <br>
     * @throws IncompatibleTypeException
     *             Case comparison cannot be made
     */
    public CMP_CLOCK compareTo(CausalityClock dvv) {
        DottedVersionVector cc = (DottedVersionVector) dvv;
        boolean thisIncludedInOther = false;
        boolean otherIncludedInThis = false;
        if (cc.ts.equals(ts)) {
            return CMP_CLOCK.CMP_EQUALS;
        }

        if (ts != null) {
            thisIncludedInOther = cc.getLatestCounter(ts.getIdentifier()) >= ts.getCounter();
        }
        if (cc.ts != null) {
            otherIncludedInThis = getLatestCounter(cc.ts.getIdentifier()) >= cc.ts.getCounter();
        }
        if (ts == null || cc.ts == null) {
            CMP_CLOCK c = compareToVV(cc);
            if (ts == null && cc.ts == null) {
                return c;
            }
            if (ts == null) {
                // this included in other if is dominated by the other VV or
                // if is equal to the other VV, as the other.ts will makes the
                // other dominate this
                thisIncludedInOther = c == CMP_CLOCK.CMP_ISDOMINATED || c == CMP_CLOCK.CMP_EQUALS;
            }
            if (cc.ts == null) {
                otherIncludedInThis = c == CMP_CLOCK.CMP_DOMINATES || c == CMP_CLOCK.CMP_EQUALS;
            }

        }

        if ((!thisIncludedInOther) && (!otherIncludedInThis)) {
            return CMP_CLOCK.CMP_CONCURRENT;
        }
        if (!thisIncludedInOther) {
            return CMP_CLOCK.CMP_DOMINATES;
        }
        if (!otherIncludedInThis) {
            return CMP_CLOCK.CMP_ISDOMINATED;
        }
        return CMP_CLOCK.CMP_EQUALS;
    }

    /**
     * Checks if a given event clock is reflected in this clock
     * 
     * @param c
     *            Event clock.
     * @return Returns true if the given event clock is included in this
     *         causality clock.
     * @throws IncompatibleTypeException
     */
    @Override
    public boolean includes(Timestamp cc) {
        if (cc.equals(ts)) {
            return true;
        }
        Long i = vv.get(cc.getIdentifier());
        return i != null && i > cc.getCounter();
    }

    /**
     * Merge this clock with the given c clock. TODO: This is inefficient.
     * 
     * @param c
     *            Clock to merge to
     * @return Returns one of the following, based on the initial value of
     *         clocks:<br>
     *         CMP_EQUALS : if clocks were equal; <br>
     *         CMP_DOMINATES : if this clock dominated the given c clock; <br>
     *         CMP_ISDOMINATED : if this clock was dominated by the given c
     *         clock; <br>
     *         CMP_CONCUREENT : if this clock and the given c clock were
     *         concurrent; <br>
     */
    protected CMP_CLOCK mergeVV(DottedVersionVector cc) {
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
     * @param c
     *            Clock to merge to
     * @return Returns one of the following, based on the initial value of
     *         clocks:<br>
     *         CMP_EQUALS : if clocks were equal; <br>
     *         CMP_DOMINATES : if this clock dominated the given c clock; <br>
     *         CMP_ISDOMINATED : if this clock was dominated by the given c
     *         clock; <br>
     *         CMP_CONCUREENT : if this clock and the given c clock were
     *         concurrent; <br>
     */
    public CMP_CLOCK merge(CausalityClock dvv) {
        DottedVersionVector cc = (DottedVersionVector) dvv;
        CMP_CLOCK result = compareTo(cc);
        cc.normalize();
        this.normalize();
        mergeVV(cc);
        ts = null;
        return result;
    }

    /**
     * Create a copy of this causality clock.
     */
    public CausalityClock clone() {
        return new DottedVersionVector(this);
    }

    @Override
    public Timestamp getLatest(String siteid) {
        if (ts != null && ts.getIdentifier().equals(siteid)) {
            return ts;
        }
        Long i = vv.get(siteid);
        if (i == null) {
            return new Timestamp(siteid, Timestamp.MIN_VALUE);
        } else {
            return new Timestamp(siteid, i);
        }
    }

    @Override
    public long getLatestCounter(String siteid) {
        if (ts != null && ts.getIdentifier().equals(siteid)) {
            return ts.getCounter();
        }
        Long i = vv.get(siteid);
        if (i == null) {
            return Timestamp.MIN_VALUE;
        } else {
            return i;
        }
    }

}
