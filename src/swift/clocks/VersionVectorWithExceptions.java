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
    protected TreeMap<String, Set<Long>> excludedTimestamps;

    public VersionVectorWithExceptions() {
        super();
        excludedTimestamps = new TreeMap<String, Set<Long>>();
    }

    public VersionVectorWithExceptions(VersionVectorWithExceptions v) {
        super(v);
        excludedTimestamps = new TreeMap<String, Set<Long>>(v.excludedTimestamps);
    }

    public VersionVectorWithExceptions(VersionVector v) {
        super(v);
        excludedTimestamps = new TreeMap<String, Set<Long>>();
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
        if (!super.includes(cc)) {
            return false;
        }

        Set<Long> siteExcludes = excludedTimestamps.get(cc.getIdentifier());
        return siteExcludes == null || !siteExcludes.contains(cc.getCounter());
    }

    /**
     * Records an event.
     * 
     * @param cc
     *            Timestamp to insert.
     * @throws InvalidParameterException
     */
    public boolean record(Timestamp cc) {
        Set<Long> set = excludedTimestamps.get(cc.getIdentifier());
        if (set != null) {
            if (set.remove(cc.getCounter())) {
                return true;
            }
        }
        long prev = super.getLatestCounter(cc.getIdentifier()) + 1;
        if (!super.record(cc)) {
            return false;
        }
        long fol = cc.getCounter();
        if (prev <= fol) {
            return true;
        }
        if (set == null) {
            set = new TreeSet<Long>();
            excludedTimestamps.put(cc.getIdentifier(), set);
        }
        for (; prev < fol; prev++) {
            set.add(prev);
        }
        return true;
    }
    
    protected CMP_CLOCK mergeOneEntryVV( String siteid, long last, Set<Long> excluded) {
        Long i = vv.get( siteid);
        Set<Long> excludedThis = excludedTimestamps.get(siteid);
        if( i == null) {
            vv.put(siteid, last);
            if (excludedThis == null) {
                if (excluded != null) {
                    excludedTimestamps.put(siteid, new TreeSet<Long>(excluded));
                }
            } else {    // should not happen !!!
                if (excluded != null) {
                    excludedThis.addAll(excluded);
                }
            }
            return CMP_CLOCK.CMP_ISDOMINATED;
        }
        long iL = i;
        
        boolean lessThan = false;
        boolean greaterThan = false;

        if( excludedThis != null) {                // remove from excluded elements those that are reflected in cc
            Iterator<Long> itS = excludedThis.iterator();
            while( itS.hasNext()) {
                long l = itS.next();
                if( l <= last && excluded != null && ! excluded.contains( new Long(l))) {
                    itS.remove();
                    lessThan = true;
                }
            }
        }
        if( excluded != null && last > iL) {  // add excluded elements that are larger than local max value                     Iterator<Long> itS = sThis.iterator();
            Iterator<Long> itS = excluded.iterator();
            while( itS.hasNext()) {
                long l = itS.next();
                 if( l > iL) {
                     excludedThis.add(l);
                     greaterThan = true;
                }
            }
        }
        if (iL < last) {
            lessThan = true;
            vv.put(siteid, last);
        } else if (iL > last) {
            greaterThan = true;
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
     * @throws IncompatibleTypeException
     *             Case comparison cannot be made
     */
    protected CMP_CLOCK mergeVV(VersionVectorWithExceptions cc) {
        CMP_CLOCK result = CMP_CLOCK.CMP_EQUALS;
        Iterator<Entry<String, Long>> it = cc.vv.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Long> e = it.next();
            CMP_CLOCK partialResult = mergeOneEntryVV( e.getKey(), e.getValue(), cc.excludedTimestamps.get(e.getKey()));
            result = ClockUtils.combineCmpClock(result, partialResult);
        }
        it = vv.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Long> e = it.next();
            Long i = cc.vv.get(e.getKey());
            if (i == null) {
                result = ClockUtils.combineCmpClock(result, CMP_CLOCK.CMP_DOMINATES);
                break;
            }
        }
        return result;
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
    public CMP_CLOCK merge(VersionVectorWithExceptions cc) {
        // if ( ! VersionVectorWithExceptions.class.equals(cc.getClass())) {
        // throw new IncompatibleTypeException();
        // }
        return mergeVV(cc);
    }

    protected CMP_CLOCK compareOneEntryVV(String siteid, long last, Set<Long> excluded) {
        Long i = vv.get(siteid);
        Set<Long> excludedThis = excludedTimestamps.get(siteid);
        if (i == null) {
            return CMP_CLOCK.CMP_ISDOMINATED;
        }
        long iL = i;
        
        boolean lessThan = false;
        boolean greaterThan = false;

        if( excludedThis != null) {                // remove from excluded elements those that are reflected in cc
            Iterator<Long> itS = excludedThis.iterator();
            while( itS.hasNext()) {
                long l = itS.next();
                if( l <= last && excluded != null && ! excluded.contains( new Long(l))) {
                    lessThan = true;
                }
            }
        }
        if( excluded != null && last > iL) {  // add excluded elements that are larger than local max value                     Iterator<Long> itS = sThis.iterator();
            Iterator<Long> itS = excluded.iterator();
            while( itS.hasNext()) {
                long l = itS.next();
                 if( l > iL) {
                     greaterThan = true;
                }
            }
        }
        if (iL < last) {
            lessThan = true;
        } else if (iL > last) {
            greaterThan = true;
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
     * compare this clock with the given c clock.
     * 
     * @param c
     *            Clock to compare to
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
    protected CMP_CLOCK compareVV(VersionVectorWithExceptions cc) {
        CMP_CLOCK result = CMP_CLOCK.CMP_EQUALS;
        Iterator<Entry<String, Long>> it = cc.vv.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Long> e = it.next();
            CMP_CLOCK partialResult = compareOneEntryVV( e.getKey(), e.getValue(), cc.excludedTimestamps.get(e.getKey()));
            result = ClockUtils.combineCmpClock(result, partialResult);
        }
        it = vv.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Long> e = it.next();
            Long i = cc.vv.get(e.getKey());
            if (i == null) {
                result = ClockUtils.combineCmpClock(result, CMP_CLOCK.CMP_DOMINATES);
                break;
            }
        }
        return result;
    }

    //TODO: fix parametric types
    @Override
    public CMP_CLOCK compareTo(VersionVector cc) {
        // if ( ! VersionVectorWithExceptions.class.equals(cc.getClass())) {
        // throw new IncompatibleTypeException();
        // }
        return compareVV((VersionVectorWithExceptions)cc);
    }


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
