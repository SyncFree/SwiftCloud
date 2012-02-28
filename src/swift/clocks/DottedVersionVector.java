package swift.clocks;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import swift.exceptions.IncompatibleTypeException;

/**
 * Class to represent a dotted version vector.<br>
 * A dotted version vector extends a common version vector with a timestamp that
 * can be decoupled from the normal sequence.
 * 
 * @author nmp
 */
public class DottedVersionVector extends VersionVector {
    protected Timestamp ts;

    public DottedVersionVector(Timestamp ts) {
        this.ts = ts;
    }

    public DottedVersionVector(DottedVersionVector v) {
        super(v);
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
        if (super.getLatestCounter(c.getIdentifier()) >= c.getCounter()) {
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
    public CMP_CLOCK compareTo(DottedVersionVector cc) {
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
            CMP_CLOCK c = super.compareToVV(cc);
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
    public CMP_CLOCK merge(DottedVersionVector cc) {
        CMP_CLOCK result = compareTo(cc);
        cc.normalize();
        this.normalize();
        super.mergeVV(cc);
        ts = null;
        return result;
    }

    /**
     * Create a copy of this causality clock.
     */
    public CausalityClock<VersionVector> clone() {
        return new DottedVersionVector(this);
    }

    @Override
    public Timestamp getLatest(String siteid) {
        if (ts != null && ts.getIdentifier().equals(siteid)) {
            return ts;
        }
        return super.getLatest(siteid);
    }

    @Override
    public long getLatestCounter(String siteid) {
        if (ts != null && ts.getIdentifier().equals(siteid)) {
            return ts.getCounter();
        }
        return super.getLatestCounter(siteid);
    }

}
