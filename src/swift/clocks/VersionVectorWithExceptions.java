package swift.clocks;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.exceptions.IncompatibleTypeException;
import swift.exceptions.InvalidParameterException;

/**
 * Class to represent common version vectors.
 * 
 * @author nmp
 */
public class VersionVectorWithExceptions extends VersionVector {

    private static final long serialVersionUID = 1L;
    protected TreeMap<String, Set<Timestamp>> excludedTimestamps;

    public VersionVectorWithExceptions() {
        super();
        excludedTimestamps = new TreeMap<String, Set<Timestamp>>();
    }

    public VersionVectorWithExceptions(VersionVectorWithExceptions v) {
        super( v);
        excludedTimestamps = new TreeMap<String, Set<Timestamp>>(
                v.excludedTimestamps);
    }

    public VersionVectorWithExceptions(VersionVector v) {
        super( v);
        excludedTimestamps = new TreeMap<String, Set<Timestamp>>();
    }

    /**
     * Checks if a given event clock is reflected in this clock
     * 
     * @param c
     *            Event clock.
     * @return Returns true if the given event clock is included in this
     *         causality clock.
     */
    @Override
    public boolean includes(Timestamp cc) {
        if (! super.includes(cc)) {
            return true;
        }

        Set<Timestamp> siteExcludes = excludedTimestamps.get(cc.getIdentifier());
        return siteExcludes == null || ! siteExcludes.contains(cc);
    }

    /**
     * Records an event.
     * 
     * @param cc
     *            Timestamp to insert.
     * @throws InvalidParameterException
     */
    public boolean record(Timestamp cc)  {
        Set<Timestamp> set = excludedTimestamps.get(cc.getIdentifier());
        if (set != null) {
            if (set.remove(cc)) {
                return true;
            }
        }
        Timestamp lastCC = super.getLatest(cc.getIdentifier());
        if (! super.record(lastCC)) {
        	return false;
        }
        long prev = lastCC.getCounter() + 1;
        long fol = cc.getCounter();
        if (prev <= fol) {
            return true;
        }
        if (set == null) {
            set = new TreeSet<Timestamp>();
            excludedTimestamps.put(cc.getIdentifier(), set);
        }
        for ( ; prev < fol; prev++) {
            set.add(new Timestamp( cc.getIdentifier(), prev));
        }
        return true;
    }

    /**
     * Excludes the given Timestamp from this version vector<br>
     * Is this used for anything?
     * @param cc
     */
    public void exclude(Timestamp cc) {
        Timestamp lastCC = super.getLatest(cc.getIdentifier());
        if( lastCC.getCounter() < cc.getCounter())
            return;
        Set<Timestamp> excludes = excludedTimestamps.get(cc.getIdentifier());
        if (excludes == null) {
            excludes = new TreeSet<Timestamp>();
            excludedTimestamps.put(cc.getIdentifier(), excludes);
        }
        excludes.add(cc);
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
    public CMP_CLOCK merge(VersionVectorWithExceptions c)
            throws IncompatibleTypeException {
        VersionVectorWithExceptions cc = (VersionVectorWithExceptions) c;
        boolean lessThan = false; // this less than c
        boolean greaterThan = false;
        Iterator<Entry<String, Long>> it = cc.vv.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Long> e = it.next();
            Long i = vv.get(e.getKey());
            if (i == null) {
                lessThan = true;
                vv.put(e.getKey(), e.getValue());
                if (cc.excludedTimestamps.containsKey(e.getKey())) {
                    excludedTimestamps.put(e.getKey(), new TreeSet<Timestamp>(
                            cc.excludedTimestamps.get(e.getKey())));
                }

            } else {
                long iOther = e.getValue();
                long iThis = i;
                if (iThis < iOther) {
                    lessThan = true;
                    vv.put(e.getKey(), iOther);
                    excludedTimestamps.put(e.getKey(), new HashSet<Timestamp>(
                            cc.excludedTimestamps.get(e.getKey())));
                } else if (iThis > iOther) {
                    greaterThan = true;
                } else {
                    Set<Timestamp> exc = excludedTimestamps.get(e.getKey());
                    Set<Timestamp> otherExc = cc.excludedTimestamps.get(e.getKey());
                    if (exc != null
                            && exc.contains(iThis)
                            && (otherExc == null || otherExc != null
                                    && !otherExc.contains(iThis))) {
                        exc.remove(iThis);
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
            } else {
                long iThis = e.getValue();
                long iOther = i;
                if (iThis < iOther) {
                    lessThan = true;
                    vv.put(e.getKey(), iOther);
                    excludedTimestamps.put(e.getKey(), new HashSet<Timestamp>(
                            cc.excludedTimestamps.get(e.getKey())));
                } else if (iThis > iOther) {
                    greaterThan = true;
                } else {
                    Set<Timestamp> exc = excludedTimestamps.get(e.getKey());
                    Set<Timestamp> otherExc = cc.excludedTimestamps.get(e.getKey());
                    if (exc != null
                            && exc.contains(iThis)
                            && (otherExc == null || otherExc != null
                                    && !otherExc.contains(iThis))) {
                        exc.remove(iThis);
                    }
                }
            }
        }
        /*
         * // aqui só adiciona as novas excepções for (Entry<String,
         * Set<Long>> entry : cc.excludedTimestamps.entrySet()) { Set<Long>
         * otherExcluded = entry.getValue(); Set<Long> siteExcluded =
         * excludedTimestamps.get(entry.getKey()); if (siteExcluded == null) {
         * siteExcluded = new HashSet<Long>();
         * excludedTimestamps.put(entry.getKey(), siteExcluded); }
         * 
         * for (Long excluded : otherExcluded) {
         * 
         * } }
         */
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

    @Override
    public CMP_CLOCK compareTo(VersionVector c) {
        return CMP_CLOCK.CMP_CONCURRENT;
/*
        // TODO Autsch! Fix me!
        VersionVectorWithExceptions cc = (VersionVectorWithExceptions) c;
        boolean lessThan = false; // this less than c
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

        for (Entry<String, Set<Long>> entry : excludedTimestamps.entrySet()) {
            Set<Long> excluded = entry.getValue();
            Long iOther = cc.vv.get(entry.getKey());
            for (Long i : excluded) {
                if (iOther != null && iOther >= i) {
                    Set<Timestamp> otherExcluded = cc.excludedTimestamps.get(entry
                            .getKey());
                    if (otherExcluded == null
                            || (otherExcluded != null && !otherExcluded
                                    .contains(i)))
                        lessThan = true;
                }
            }
        }

        for (Entry<String, Set<Timestamp>> entry : cc.excludedTimestamps.entrySet()) {
            Set<Timestamp> otherExcluded = entry.getValue();
            Long iThis = vv.get(entry.getKey());
            for (Timestamp i : otherExcluded) {
                if (iThis != null && iThis >= i) {
                    Set<Long> myExcluded = excludedTimestamps.get(entry
                            .getKey());
                    if (myExcluded == null
                            || (myExcluded != null && !myExcluded.contains(i)))
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

        for (Entry<String, Set<Long>> entry : excludedTimestamps.entrySet()) {
            Set<Long> excluded = entry.getValue();
            Set<Long> otherExcluded = cc.excludedTimestamps.get(entry.getKey());
            // TODO refactor
            if (excluded.containsAll(otherExcluded)) {
                if (otherExcluded.containsAll(excluded)) {
                    if (excluded.size() < otherExcluded.size()) {
                        greaterThan = true;
                    }
                }
            }
            if (excluded.containsAll(otherExcluded)) {
                if (excluded.size() > otherExcluded.size()) {
                    lessThan = true;
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
        */
    }

    /**
     * Returns the number of operations reflected in one vector that are not
     * reflected in the other.
     * 
     * @param c
     *            Clock to compare to
     * @return
     * @throws IncompatibleTypeException
     *             Case comparison cannot be made
     */

    // TODO:public long difference(CausalityClock c) throws
    // IncompatibleTypeException {
    // }

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
        buf.append("X:[" + excludedTimestamps + "]");
        return buf.toString();
    }


}
