package swift.clocks;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import swift.exceptions.IncompatibleTypeException;

/**
 * Class to represent common version vectors.
 * 
 * @author nmp
 */
public class DottedVersionVector extends VersionVector {
    protected Timestamp ts;

    public DottedVersionVector( Timestamp ts) {
        this.ts = ts;
    }

    public DottedVersionVector(DottedVersionVector v) {
        super( v);
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
     * IMPORTANT NOTE: Assumes no holes in event history.
     * 
     * @param siteid
     *            Site identifier.
     * @return Returns an event clock.
     * @throws IncompatibleTypeException
     * @deprecated Use TimestampSource instead
     */
    @Deprecated
    public Timestamp recordNext(String siteid) {
        normalize();
        Long i = vv.get(siteid);
        ts = new Timestamp(siteid, ++i);
        return ts;
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
    public void record(Timestamp c) {
        Timestamp cc = (Timestamp) c;
        normalize();
        ts = cc;
    }

    /**
     * Returns true if vv1 <= vv2, i.e., vv1 is equal or dominated by vv2
     * 
     * @param vv1
     * @param vv1
     * @return
     */
    protected boolean isEqualOrDominatedLessSite(Map<String, Long> vv1,
            Map<String, Long> vv2, String siteid) {
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
        if (ts != null && cc.ts != null) {
            Long i1 = cc.vv.get(ts.getIdentifier());
            thisIncludedInOther = i1 != null && i1 >= ts.getCounter();
            Long i2 = vv.get(cc.ts.getIdentifier());
            otherIncludedInThis = i2 != null && i2 >= cc.ts.getCounter();
            if (ts.getIdentifier().equals(cc.ts.getIdentifier())
                    && ts.getCounter() == cc.ts.getCounter()) {
                return CMP_CLOCK.CMP_EQUALS;
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
        } else if (ts != null) { // cc.ts == null
            Long iOther = cc.vv.get(ts.getIdentifier());
            thisIncludedInOther = iOther != null && iOther >= ts.getCounter();
            if (isEqualOrDominatedLessSite(cc.vv, vv, ts.getIdentifier())) {
                // cc.vv <=
                // vv (for
                // all less
                // siteid)
                Long iThis = vv.get(ts.getIdentifier());
                if (iThis == null && iOther == null) {
                    otherIncludedInThis = true;
                } else if (iThis != null && iOther == null) {
                    otherIncludedInThis = true;
                } else if (iThis == null && iOther != null) {
                    otherIncludedInThis = ts.getCounter() == iOther;
                    // not sure
                    // about this.
                    // e.g. dvv1 =
                    // [](2,site0)
                    // dvv2 =
                    // [(site0,2)]
                    // event
                    // (1,site0) is
                    // not
                    // necessarily
                    // included in
                    // dvv1
                } else {
                    otherIncludedInThis = ts.getCounter() == iOther
                            || iThis >= iOther;
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
        } else if (cc.ts != null) {
            // ts == null
            Long iThis = vv.get(cc.ts.getIdentifier());
            otherIncludedInThis = iThis != null && iThis >= cc.ts.getCounter();
            if (isEqualOrDominatedLessSite(vv, cc.vv, cc.ts.getIdentifier())) {
                // vv <=
                // cc.vv
                // (for
                // all
                // less
                // siteid)
                Long iOther = cc.vv.get(cc.ts.getIdentifier());
                if (iThis == null && iOther == null) {
                    thisIncludedInOther = true;
                } else if (iThis == null && iOther != null) {
                    thisIncludedInOther = true;
                } else if (iOther == null && iThis != null) {
                    thisIncludedInOther = ts.getCounter() == iThis;
                    // not sure about
                    // this. e.g.
                    // dvv1 =
                    // [](2,site0)
                    // dvv2 =
                    // [(site0,2)]
                    // event
                    // (1,site0) is
                    // not
                    // necessarily
                    // included in
                    // dvv1
                } else {
                    thisIncludedInOther = cc.ts.getCounter() == iThis
                            || iOther >= iThis;
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
        } else {
            // ts == null AND cc.ts == NULL
            // VV rules
            boolean lessThan = false;
            // this less than c
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
            it = vv.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, Long> e = it.next();
                Long i = cc.vv.get(e.getKey());
                if (i == null) {
                    greaterThan = true;
                    if (lessThan) {
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
     * @throws IncompatibleTypeException
     *             Case comparison cannot be made
     */
    public CMP_CLOCK merge(DottedVersionVector cc) {
        CMP_CLOCK result = compareTo(cc);
        cc.normalize();
        this.normalize();
        // boolean lessThan = false; // this less than c
        // boolean greaterThan = false;
        Iterator<Entry<String, Long>> it = cc.vv.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Long> e = it.next();
            long value = cc.ts != null
                    && cc.ts.getIdentifier().equals(e.getKey()) ? cc.ts
                    .getCounter() : e.getValue();
            Long i = vv.get(e.getKey());
            if (i == null) {
                // lessThan = true;
                vv.put(e.getKey(), value);
            } else {
                long iThis = value;
                long iOther = i;
                if (iThis < iOther) {
                    // lessThan = true;
                    vv.put(e.getKey(), iOther);
                } else if (iThis > iOther) {
                    // greaterThan = true;
                }
            }
        }
        it = vv.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Long> e = it.next();
            Long i = cc.vv.get(e.getKey());
            if (i == null) {
                // greaterThan = true;
            } else {
                long iOther = e.getValue();
                long iThis = i;
                if (iThis < iOther) {
                    // lessThan = true;
                    vv.put(e.getKey(), iOther);
                } else if (iThis > iOther) {
                    // greaterThan = true;
                }
            }
        }
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
