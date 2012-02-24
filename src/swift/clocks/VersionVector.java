package swift.clocks;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import swift.exceptions.IncompatibleTypeException;
import swift.exceptions.InvalidParameterException;

/**
 * Class to represent common version vectors.
 * 
 * @author nmp
 */
public class VersionVector implements CausalityClock<VersionVector> {

    private static final long serialVersionUID = 1L;
    protected Map<String, Long> vv;

    public VersionVector() {
        vv = new TreeMap<String, Long>();
    }

    public VersionVector(VersionVector v) {
        vv = new TreeMap<String, Long>(v.vv);
    }

    /**
     * Records the next event. <br>
     * IMPORTANT NOTE: Assumes no holes in event history.
     * 
     * @param siteid
     *            Site identifier.
     * @return Returns an event clock.
     */
    @Deprecated
    public Timestamp recordNext(String siteid) {
        Long i = vv.get(siteid);
        if (i == null) {
            i = new Long(0);
        }
        Timestamp ts = new Timestamp(siteid, ++i);
        vv.put(ts.getIdentifier(), ts.getCounter());
        return ts;
    }

    /**
     * Records an event. <br>
     * IMPORTANT NOTE: Assumes no holes in event history.
     * 
     * @param ec
     *            Event clock.
     * @throws InvalidParameterException
     */
    public void record(Timestamp cc) throws InvalidParameterException {
        Long i = vv.get(cc.getIdentifier());
        if (i == null || i < cc.getCounter()) {
            vv.put(cc.getIdentifier(), cc.getCounter());
        } else {
            throw new InvalidParameterException();
        }
    }

    /**
     * Returns the most recent event for a given site. <br>
     * @param siteid
     *            Site identifier.
     * @return Returns an event clock.
     */
    public Timestamp getLatest(String siteid) {
        Long i = vv.get(siteid);
        if (i == null) {
            return new Timestamp(siteid, 0);
        } else {
            return new Timestamp(siteid, i);
        }
    }

    /**
     * Returns the most recent event for a given site. <br>
     * @param siteid
     *            Site identifier.
     * @return Returns an event clock.
     */
    public long getLatestCounter(String siteid) {
        Long i = vv.get(siteid);
        if (i == null) {
            return 0;
        } else {
            return i;
        }
    }

    /**
     * Compares two causality clock.
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
    public CMP_CLOCK compareTo(VersionVector cc)
            throws IncompatibleTypeException {
        boolean lessThan = false;
        boolean greaterThan = false;
        Iterator<Entry<String, Long>> it = cc.vv.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Long> e = it.next();
            Long i = vv.get(e.getKey());
            if (i == null) {
                lessThan = true;
                if (greaterThan) {
                    return CMP_CLOCK.CMP_CONCURRENT;
                }
            } else {
                long iOther = e.getValue();
                long iThis = i;
                if (iThis < iOther) {
                    lessThan = true;
                    if (greaterThan) {
                        return CMP_CLOCK.CMP_CONCURRENT;
                    }
                } else if (iThis > iOther) {
                    greaterThan = true;
                    if (lessThan) {
                        return CMP_CLOCK.CMP_CONCURRENT;
                    }
                }
            }
        }
        it = this.vv.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Long> e = it.next();
            Long i = cc.vv.get(e.getKey());
            if (i == null) {
                greaterThan = true;
                if (lessThan) {
                    return CMP_CLOCK.CMP_CONCURRENT;
                }
            } else {
                long iThis = e.getValue();
                long iOther = i;
                if (iThis < iOther) {
                    lessThan = true;
                    if (greaterThan) {
                        return CMP_CLOCK.CMP_CONCURRENT;
                    }
                } else if (iThis > iOther) {
                    greaterThan = true;
                    if (lessThan) {
                        return CMP_CLOCK.CMP_CONCURRENT;
                    }
                }
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
        Long i = vv.get(cc.getIdentifier());
        return i != null && cc.getCounter() <= i;
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
     * @throws IncompatibleTypeException
     *             Case comparison cannot be made
     */
    public CMP_CLOCK merge(VersionVector cc) {
        boolean lessThan = false; // this less than c
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
            } else {
                long iThis = e.getValue();
                long iOther = i;
                if (iThis < iOther) {
                    lessThan = true;
                    vv.put(e.getKey(), iOther);
                } else if (iThis > iOther) {
                    greaterThan = true;
                }
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
     * Returns the number of operations reflected in one vector that are not
     * reflected in the other.
     * 
     * @param c
     *            Clock to compare to
     * @return
     */
    @Deprecated
    public long difference(VersionVector cc) {
        Set<String> s = new HashSet<String>();
        s.addAll(cc.vv.keySet());
        s.addAll(vv.keySet());
        long dif = 0;
        Iterator<String> it = s.iterator();
        while (it.hasNext()) {
            String k = it.next();
            Long localV = vv.get(k);
            Long remV = cc.vv.get(k);
            dif = dif
                    + Math.abs((localV == null ? 0 : localV)
                            - (remV == null ? 0 : remV));
        }
        return dif;
    }

    /**
     * Create a copy of this causality clock.
     */
    public CausalityClock<VersionVector> clone() {
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

}
