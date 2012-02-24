package swift.clocks;

import java.util.Iterator;
import java.util.Map.Entry;

import swift.exceptions.InvalidParameterException;

/**
 * Class to represent common version vectors.
 * 
 * @author nmp
 * @deprecated use VersionVector instead
 */
@Deprecated
public class GlobalVersionVector extends VersionVector {

    private static final long serialVersionUID = 1L;
    Long maxCounter = -1L;

    public GlobalVersionVector() {
        super();
    }

    public GlobalVersionVector(GlobalVersionVector other) {
        super(other);
        maxCounter = other.maxCounter;
    }

    /**
     * Records the next event. <br>
     * IMPORTANT NOTE: Assumes no holes in event history.
     * 
     * @param siteid
     *            Site identifier.
     * @return Returns an event clock.
     * @throws IncompatibleTypeException
     */
    public Timestamp recordNext(String siteid) {
        Timestamp ts = new Timestamp(siteid, ++maxCounter);
        vv.put(ts.getIdentifier(), ts.getCounter());
        return ts;
    }

    /**
     * Records the next event. <br>
     * IMPORTANT NOTE: Assumes no holes in event history.
     * 
     * @param ec
     *            Event clock.
     * @return Returns true if the given event clock is included in this
     *         causality clock.
     * @throws InvalidParameterException
     * @throws IncompatibleTypeException
     */
    public void record(Timestamp cc) throws InvalidParameterException {
        Long i = vv.get(cc.getIdentifier());
        if (i == null || i < cc.getCounter()) {
            vv.put(cc.getIdentifier(), cc.getCounter());
            if (cc.getCounter() > maxCounter) {
                maxCounter = cc.getCounter();
            }
        } else {
            throw new InvalidParameterException();
        }
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
    public CMP_CLOCK merge(GlobalVersionVector cc) {
        boolean lessThan = false; // this less than c
        boolean greaterThan = false;
        Iterator<Entry<String, Long>> it = cc.vv.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Long> e = it.next();
            Long i = vv.get(e.getKey());
            if (i == null) {
                lessThan = true;
                vv.put(e.getKey(), e.getValue());
                if (e.getValue() > maxCounter) {
                    maxCounter = e.getValue();
                }
            } else {
                long iOther = e.getValue();
                long iThis = i;
                if (iThis < iOther) {
                    lessThan = true;
                    vv.put(e.getKey(), iOther);
                    if (e.getValue() > maxCounter) {
                        maxCounter = iOther;
                    }
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
            } else {
                long iThis = e.getValue();
                long iOther = i;
                if (iThis < iOther) {
                    lessThan = true;
                    vv.put(e.getKey(), iOther);
                    if (iOther > maxCounter) {
                        maxCounter = iOther;
                    }
                } else if (iThis > iOther) {
                    greaterThan = true;
                }
            }
        }
        // TODO Check code duplication
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
     * Create a copy of this causality clock.
     */
    @Override
    public CausalityClock<VersionVector> clone() {
        return new GlobalVersionVector(this);
    }

    public Timestamp getMaxTimestamp() {
        Timestamp max = null;
        for (Entry<String, Long> e : vv.entrySet()) {
            if (max == null) {
                max = new Timestamp(e.getKey(), e.getValue());
            } else {
                Timestamp tmp = new Timestamp(e.getKey(), e.getValue());
                if (tmp.compareTo(max) > 0) {
                    max = tmp;
                }
            }
        }
        return max;
    }

}
