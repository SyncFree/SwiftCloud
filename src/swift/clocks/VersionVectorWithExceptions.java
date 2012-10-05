package swift.clocks;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import swift.exceptions.IncompatibleTypeException;

/**
 * Class to represent version vectors with exceptions. This representation
 * records the intervals of contiguous values.
 *
 * @author nmp
 */
public class VersionVectorWithExceptions implements CausalityClock {

    @Override
    public Object copy() {
        return new VersionVectorWithExceptions(this);
    }

    public static class Interval {

        long from; // inclusive
        long to; // inclusive

        Interval() {
        }

        Interval(long from, long to) {
            assert (from <= to);
            this.from = from;
            this.to = to;
        }

        boolean includes(long l) {
            return l >= from && l <= to;
        }

        boolean mergeFwd(Interval p) {
            if (to == p.from + 1) {
                to = p.to;
                return true;
            } else {
                return false;
            }
        }

        boolean mergeBack(Interval p) {
            if (from == p.to + 1) {
                from = p.from;
                return true;
            } else {
                return false;
            }
        }

        Interval duplicate() {
            return new Interval(from, to);
        }

        public long getFrom() {
            return from;
        }

        public long getTo() {
            return to;
        }

        @Override
        public String toString() {
            return "[" +from + "-"+ to + ']';
        }

        @Override
        public int hashCode() {
            int hash = 5;
            hash = 97 * hash + (int) (this.from ^ (this.from >>> 32));
            hash = 97 * hash + (int) (this.to ^ (this.to >>> 32));
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final Interval other = (Interval) obj;
            if (this.from != other.from) {
                return false;
            }
            if (this.to != other.to) {
                return false;
            }
            return true;
        }
        
        
    }
    private static final long serialVersionUID = 1L;
    protected Map<String, LinkedList<Interval>> vv;
    // total number of intervals
    protected int numPairs;

    public VersionVectorWithExceptions() {
        vv = new TreeMap<String, LinkedList<Interval>>();
        numPairs = 0;
    }

    protected VersionVectorWithExceptions(VersionVectorWithExceptions v) {
        vv = new TreeMap<String, LinkedList<Interval>>();
        numPairs = v.numPairs;
        for (Entry<String, LinkedList<Interval>> entry : v.vv.entrySet()) {
            String key = entry.getKey();
            LinkedList<Interval> l = entry.getValue();
            vv.put(key, duplicateList(l));
        }
    }

    protected VersionVectorWithExceptions(VersionVector v) {
        vv = new TreeMap<String, LinkedList<Interval>>();
        numPairs = 0;
        for (Entry<String, Long> entry : v.vv.entrySet()) {
            String key = entry.getKey();
            LinkedList<Interval> nl = new LinkedList<Interval>();
            nl.add(new Interval(0, entry.getValue()));
            numPairs++;
            vv.put(key, nl);
        }
    }

    /**
     * Generate a deep copy of linked list.
     *
     * @param l list to be copied
     * @return deep copy
     */
    protected static LinkedList<Interval> duplicateList(List<Interval> l) {
        if (l == null) {
            return null;
        }
        LinkedList<Interval> nl = new LinkedList<Interval>();
        for (Interval p : l) {
            nl.addLast(p.duplicate());
        }
        return nl;
    }

    /**
     * Checks if a given event clock is reflected in this clock
     *
     * @param c Event clock.
     * @return Returns true if the given event clock is included in this
     * causality clock.
     */
    @Override
    public boolean includes(Timestamp cc) {
        LinkedList<Interval> l = vv.get(cc.getIdentifier());
        if (l == null) {
            return false;
        }
        long v = cc.getCounter();
        ListIterator<Interval> it = l.listIterator(l.size());
        while (it.hasPrevious()) {
            Interval p = it.previous();
            if (v > p.to) {
                return false;
            }
            if (v >= p.from) {
                return true;
            }
        }
        return false;
    }

    /**
     * Records an event.
     *
     * @param cc Timestamp to insert.
     */
    @Override
    public boolean record(Timestamp cc) {
        long v = cc.getCounter();
        LinkedList<Interval> l = vv.get(cc.getIdentifier());

        // first timestamp registered for site
        if (l == null) {
            l = new LinkedList<Interval>();
            vv.put(cc.getIdentifier(), l);
            l.add(new Interval(v, v));
            return true;
        }

        // iterate backwards through the list
        ListIterator<Interval> it = l.listIterator(l.size());
        Interval p;
        while (it.hasPrevious()) {
            p = it.previous();
            if (v >= p.from && v <= p.to) {
                // timestamp is already registered
                return false;
            }
            if (v == p.from - 1) { // v[-p-]
                p.from = p.from - 1;
                if (it.hasPrevious()) { //[-prev-]v[-p-]
                    Interval prev=it.previous();
                    if (p.mergeBack(prev)){ //[-prev-][v-p-]
                        it.remove();
                        numPairs--;
                    }
                }
                return true;
            } else if (v==p.to+1){//[-p-]v
                p.to=v;
               return true;
            } else if (v > p.to+1) {//stop at the correct place
                it.next();  //Before the insertion is before and no exception because p is a previous.
                it.add(new Interval(v, v));
                numPairs++;
                return true;
            }
        }

        l.addFirst(new Interval(v, v));
        numPairs++;
        return true;
    }

    // FIXME is this really needed? What is the semantics?!
    protected Interval advanceUntil(Interval p, Iterator<Interval> it, int val) {
        if (val <= p.to) {
            return p;
        }
        while (it.hasNext()) {
            p = it.next();
            if (val > p.to) {
                continue;
            }
            return p;
        }
        return null;
    }

    // FIXME Can this code be removed?
    /*
     * protected CMP_CLOCK mergeOneEntryVV(String siteid, LinkedList<Pair> l0) {
     * LinkedList<Pair> l = vv.get(siteid); if (l == null) { l =
     * duplicateList(l0); numPairs = numPairs + l0.size(); vv.put(siteid, l);
     * return CMP_CLOCK.CMP_ISDOMINATED; } boolean thisHasMoreEntries = false;
     * boolean otherHasMoreEntries = false; LinkedList<Pair> nl = new
     * LinkedList<Pair>(); Iterator<Pair> it = l.iterator(); Iterator<Pair> it0
     * = l0.iterator(); Pair np = null; Pair p = it.hasNext() ? it.next() :
     * null; Pair p0 = it0.hasNext() ? it0.next() : null; numPairs = 0; // last
     * value that has been compared between the two sets long v = Math.min(p ==
     * null ? Long.MAX_VALUE : p.from - 1, p0 == null ? Long.MAX_VALUE : p0.from
     * - 1); for (;;) { if (p == null && p0 == null) break; if (p != null && p0
     * != null) {
     * 
     * 
     * if (p.from == p0.from && p.to == p0.to) { nl.add(p); numPairs++; v =
     * p.to; p = null; p0 = null; } else { if (p.from <= v) { // we are in the
     * middle of p if (p0.from > v + 1) { thisHasMoreEntries = true; } if (p.to
     * < p0.from) { // p ends before p0 start v = p.to; p = null; } else { if
     * (p.to == p0.to) { v = p.to; p = null; p0 = null; } else if (p.to < p0.to)
     * { v = p.to; p = null; } else { v = p0.to; p0 = null; } } } else if
     * (p0.from <= v) { // we are in the middle of p0 if (p.from > v + 1) {
     * otherHasMoreEntries = true; } if (p0.to < p.from) { // p ends before p0
     * start v = p0.to; p0 = null; } else { if (p.to == p0.to) { v = p.to; p =
     * null; p0 = null; } else if (p0.to < p.to) { v = p0.to; p0 = null; } else
     * { v = p.to; p = null; } } } else { // need to advance to next intervals
     * if (p.from == p0.from) { v = p.from; } else if (p.from < p0.from) {
     * thisHasMoreEntries = true; if (p.to < p0.from) { v = p.to; p = null; }
     * else { if (p.to == p0.to) { v = p.to; p = null; p0 = null; } else if
     * (p.to < p0.to) { v = p.to; p = null; } else { v = p0.to; p0 = null; } } }
     * else { otherHasMoreEntries = true; if (p0.to < p.from) { v = p0.to; p0 =
     * null; } else { if (p.to == p0.to) { v = p.to; p = null; p0 = null; } else
     * if (p0.to < p.to) { v = p0.to; p0 = null; } else { v = p.to; p = null; }
     * } }
     * 
     * } } } else if (p == null) { otherHasMoreEntries = true; break; } else if
     * (p0 == null) { thisHasMoreEntries = true; break; } if (p == null &&
     * it.hasNext()) { p = it.next(); } if (p0 == null && it0.hasNext()) { p0 =
     * it0.next(); } } vv.put(siteid, nl);
     * 
     * if (thisHasMoreEntries && otherHasMoreEntries) { return
     * CMP_CLOCK.CMP_CONCURRENT; } if (thisHasMoreEntries) { return
     * CMP_CLOCK.CMP_DOMINATES; } if (otherHasMoreEntries) { return
     * CMP_CLOCK.CMP_ISDOMINATED; } return CMP_CLOCK.CMP_EQUALS; }
     * 
     * 
     * 
     * protected CMP_CLOCK mergeOneEntryVV(String siteid, LinkedList<Pair> l0) {
     * LinkedList<Pair> l = vv.get(siteid); if (l == null) { l =
     * duplicateList(l0); numPairs = numPairs + l0.size(); vv.put(siteid, l);
     * return CMP_CLOCK.CMP_ISDOMINATED; } CMP_CLOCK cmp =
     * compareOneEntryVV(siteid, l0); numPairs = numPairs - l.size();
     * LinkedList<Pair> nl = new LinkedList<Pair>(); Iterator<Pair> it =
     * l.iterator(); Iterator<Pair> it0 = l0.iterator(); Pair p = it.hasNext() ?
     * it.next() : null; Pair p0 = it0.hasNext() ? it0.next() : null; Pair np =
     * null; for (;;) { boolean hasChanged = false; if (p == null && p0 == null)
     * break; if (np == null) { if (p != null && p0 != null) { if (p.from <=
     * p0.from) { np = p; p = null; hasChanged = true; } else { np =
     * p0.duplicate(); p0 = null; hasChanged = true; } } else if (p != null) {
     * np = p; p = null; hasChanged = true; } else if (p0 != null) { np =
     * p0.duplicate(); p0 = null; hasChanged = true; } } if (p != null) { if
     * (np.to >= p.from - 1) { if (p.to > np.to) np.to = p.to; p = null;
     * hasChanged = true; } } if (p0 != null) { if (np.to >= p0.from - 1) { if
     * (p0.to > np.to) np.to = p0.to; p0 = null; hasChanged = true; } } if
     * (!hasChanged) { nl.add(np); numPairs++; np = null; } if (p == null &&
     * it.hasNext()) { p = it.next(); } if (p0 == null && it0.hasNext()) { p0 =
     * it0.next(); } } if (np != null) { nl.add(np); numPairs++; }
     * vv.put(siteid, nl);
     * 
     * return cmp; }
     */
    protected CMP_CLOCK mergeOneEntryVV(String siteid, LinkedList<Interval> l0) {
        LinkedList<Interval> l = vv.get(siteid);
        if (l == null) {
            l = duplicateList(l0);
            numPairs = numPairs + l0.size();
            vv.put(siteid, l);
            return CMP_CLOCK.CMP_ISDOMINATED;
        }
        CMP_CLOCK cmp = compareOneEntryVV(siteid, l0);
        numPairs = numPairs - l.size();
        LinkedList<Interval> nl = new LinkedList<Interval>();
        Iterator<Interval> it = l.iterator();
        Iterator<Interval> it0 = l0.iterator();
        Interval p = it.hasNext() ? it.next() : null;
        Interval p0 = it0.hasNext() ? it0.next() : null;
        Interval np = null;
        while (p != null || p0 != null) {
            boolean hasChanged = false;
            /*if (p == null && p0 == null) {
                break;
            }*/
            if (np == null) {
                if (p != null && p0 != null) {
                    if (p.from <= p0.from) {
                        np = p;
                        p = null;
                        hasChanged = true;
                    } else {
                        np = p0.duplicate();
                        p0 = null;
                        hasChanged = true;
                    }
                } else if (p != null) {
                    np = p;
                    p = null;
                    hasChanged = true;
                } else if (p0 != null) {
                    np = p0.duplicate();
                    p0 = null;
                    hasChanged = true;
                }
            }
            if (p != null) {
                if (np.to >= p.from - 1) {
                    if (p.to > np.to) {
                        np.to = p.to;
                    }
                    p = null;
                    hasChanged = true;
                }
            }
            if (p0 != null) {
                if (np.to >= p0.from - 1) {
                    if (p0.to > np.to) {
                        np.to = p0.to;
                    }
                    p0 = null;
                    hasChanged = true;
                }
            }
            if (!hasChanged) {
                nl.add(np);
                numPairs++;
                np = null;
            }
            if (p == null && it.hasNext()) {
                p = it.next();
            }
            if (p0 == null && it0.hasNext()) {
                p0 = it0.next();
            }
        }
        if (np != null) {
            nl.add(np);
            numPairs++;
        }
        vv.put(siteid, nl);

        return cmp;
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
    protected CMP_CLOCK mergeVV(VersionVectorWithExceptions cc) {
        CMP_CLOCK result = CMP_CLOCK.CMP_EQUALS;
        Iterator<Entry<String, LinkedList<Interval>>> it = cc.vv.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, LinkedList<Interval>> e = it.next();
            CMP_CLOCK partialResult = mergeOneEntryVV(e.getKey(), e.getValue());
            result = ClockUtils.combineCmpClock(result, partialResult);
        }
        it = vv.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, LinkedList<Interval>> e = it.next();
            LinkedList<Interval> l = cc.vv.get(e.getKey());
            if (l == null) {
                result = ClockUtils.combineCmpClock(result, CMP_CLOCK.CMP_DOMINATES);
                break;
            }
        }
        return result;
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
    @Override
    public CMP_CLOCK merge(CausalityClock cc) {
        // if ( ! VersionVectorWithExceptions.class.equals(cc.getClass())) {
        // throw new IncompatibleTypeException();
        // }
        return mergeVV((VersionVectorWithExceptions) cc);
    }

    protected CMP_CLOCK compareOneEntryVV(String siteid, LinkedList<Interval> l0) {
        LinkedList<Interval> l = vv.get(siteid);
        if (l == null) {
            return CMP_CLOCK.CMP_ISDOMINATED;
        }

        boolean thisHasMoreEntries = false;
        boolean otherHasMoreEntries = false;
        Iterator<Interval> it = l.iterator();
        Iterator<Interval> it0 = l0.iterator();
        Interval p = it.hasNext() ? it.next() : null;
        Interval p0 = it0.hasNext() ? it0.next() : null;
        // last value that has been compared between the two sets
        long v = Math.min(p == null ? Long.MAX_VALUE : p.from - 1, p0 == null ? Long.MAX_VALUE : p0.from - 1);
        for (;;) {
            if (p == null && p0 == null) {
                break;
            }
            if (thisHasMoreEntries && otherHasMoreEntries) {
                break;
            }
            if (p != null && p0 != null) {
                if (p.from == p0.from && p.to == p0.to) {
                    v = p.to;
                    p = null;
                    p0 = null;
                } else {
                    if (p.from <= v) { // we are in the middle of p
                        if (p0.from > v + 1) {
                            thisHasMoreEntries = true;
                        }
                        if (p.to < p0.from) { // p ends before p0 start
                            v = p.to;
                            p = null;
                        } else {
                            if (p.to == p0.to) {
                                v = p.to;
                                p = null;
                                p0 = null;
                            } else if (p.to < p0.to) {
                                v = p.to;
                                p = null;
                            } else {
                                v = p0.to;
                                p0 = null;
                            }
                        }
                    } else if (p0.from <= v) { // we are in the middle of p0
                        if (p.from > v + 1) {
                            otherHasMoreEntries = true;
                        }
                        if (p0.to < p.from) { // p ends before p0 start
                            v = p0.to;
                            p0 = null;
                        } else {
                            if (p.to == p0.to) {
                                v = p.to;
                                p = null;
                                p0 = null;
                            } else if (p0.to < p.to) {
                                v = p0.to;
                                p0 = null;
                            } else {
                                v = p.to;
                                p = null;
                            }
                        }
                    } else { // need to advance to next intervals
                        if (p.from == p0.from) {
                            v = p.from;
                        } else if (p.from < p0.from) {
                            thisHasMoreEntries = true;
                            if (p.to < p0.from) {
                                v = p.to;
                                p = null;
                            } else {
                                if (p.to == p0.to) {
                                    v = p.to;
                                    p = null;
                                    p0 = null;
                                } else if (p.to < p0.to) {
                                    v = p.to;
                                    p = null;
                                } else {
                                    v = p0.to;
                                    p0 = null;
                                }
                            }
                        } else {
                            otherHasMoreEntries = true;
                            if (p0.to < p.from) {
                                v = p0.to;
                                p0 = null;
                            } else {
                                if (p.to == p0.to) {
                                    v = p.to;
                                    p = null;
                                    p0 = null;
                                } else if (p0.to < p.to) {
                                    v = p0.to;
                                    p0 = null;
                                } else {
                                    v = p.to;
                                    p = null;
                                }
                            }
                        }

                    }
                }
            } else if (p == null) {
                otherHasMoreEntries = true;
                break;
            } else if (p0 == null) {
                thisHasMoreEntries = true;
                break;
            }
            if (p == null && it.hasNext()) {
                p = it.next();
            }
            if (p0 == null && it0.hasNext()) {
                p0 = it0.next();
            }
        }

        if (thisHasMoreEntries && otherHasMoreEntries) {
            return CMP_CLOCK.CMP_CONCURRENT;
        }
        if (thisHasMoreEntries) {
            return CMP_CLOCK.CMP_DOMINATES;
        }
        if (otherHasMoreEntries) {
            return CMP_CLOCK.CMP_ISDOMINATED;
        }
        return CMP_CLOCK.CMP_EQUALS;
    }

    /**
     * compare this clock with the given c clock.
     *
     * @param c Clock to compare to
     * @return Returns one of the following, based on the initial value of
     * clocks:<br> CMP_EQUALS : if clocks were equal; <br> CMP_DOMINATES : if
     * this clock dominated the given c clock; <br> CMP_ISDOMINATED : if this
     * clock was dominated by the given c clock; <br> CMP_CONCUREENT : if this
     * clock and the given c clock were concurrent; <br>
     * @throws IncompatibleTypeException Case comparison cannot be made
     */
    protected CMP_CLOCK compareVV(VersionVectorWithExceptions cc) {
        CMP_CLOCK result = CMP_CLOCK.CMP_EQUALS;
        for (Entry<String, LinkedList<Interval>> e : cc.vv.entrySet()) {
            CMP_CLOCK partialResult = compareOneEntryVV(e.getKey(), e.getValue());
            result = ClockUtils.combineCmpClock(result, partialResult);
        }
        // Test if there are more entries that have not been compared yet
        if (vv.size() > cc.vv.size()) {
            result = ClockUtils.combineCmpClock(result, CMP_CLOCK.CMP_DOMINATES);
        }
        return result;
    }

    // TODO: fix parametric types
    @Override
    public CMP_CLOCK compareTo(CausalityClock cc) {
        // if ( ! VersionVectorWithExceptions.class.equals(cc.getClass())) {
        // throw new IncompatibleTypeException();
        // }
        return compareVV((VersionVectorWithExceptions) cc);
    }

    /**
     * Returns the most recent event for a given site. <br>
     *
     * @param siteid Site identifier.
     * @return Returns an event clock.
     */
    @Override
    public Timestamp getLatest(String siteid) {
        return new Timestamp(siteid, getLatestCounter(siteid));
    }

    /**
     * Returns the most recent event for a given site. <br>
     *
     * @param siteid Site identifier.
     * @return Returns an event clock.
     */
    @Override
    public long getLatestCounter(String siteid) {
        LinkedList<Interval> p = vv.get(siteid);
        if (p == null) {
            return Timestamp.MIN_VALUE;
        } else {
            return p.getLast().to;
        }
    }

    @Override
    public boolean hasEventFrom(String siteid) {
        // FIXME Why not just test if siteid has entries in the vv?
        return getLatestCounter(siteid) != Timestamp.MIN_VALUE;
    }

    /**
     * Create a copy of this causality clock.
     */
    @Override
    public CausalityClock clone() {
        return new VersionVectorWithExceptions(this);
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("[");
        Iterator<Entry<String, LinkedList<Interval>>> it = vv.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, LinkedList<Interval>> e = it.next();
            buf.append(e.getKey());
            buf.append(":");
            Iterator<Interval> it2 = e.getValue().iterator();
            while (it2.hasNext()) {
                buf.append(it2.next().toString());
            }
            if (it.hasNext()) {
                buf.append(",");
            }
        }
        buf.append("]");
        return buf.toString();
    }

    @Override
    public boolean hasExceptions() {
        return vv.size() != numPairs;
    }

    @Override
    public void drop(String siteId) {
        vv.remove(siteId);
    }
    
    @Override
    public void drop(final Timestamp cc) {
        LinkedList<Interval> l = vv.get(cc.getIdentifier());
        if (l == null) {
            return;
        }
        long v = cc.getCounter();
        // iterate the list from back to front
        ListIterator<Interval> it = l.listIterator(l.size());
        while (it.hasPrevious()) {
            Interval p = it.previous();

            // timestamp is not registered in clock
            if (v > p.to) {
                return;
            } else if (v == p.to) {
                // timestamp is at beginning of interval
                p.to = p.to - 1;
                cleanUp(cc, l, it, p);
                return;
            } else if (v == p.from) {
                // timestamp is at end of interval
                p.from = p.from + 1;
                cleanUp(cc, l, it, p);
                return;
            } else if (v > p.from && v < p.to) {
                // timestamp is in the interval, so split the interval
                it.add(new Interval(p.from, v - 1));
                p.from = v + 1;
                numPairs++;
                return;
            }
        }
    }

    @Override
    public void recordAllUntil(Timestamp timestamp) {
        if (vv.containsKey(timestamp.getIdentifier())) {
            for (long i = Timestamp.MIN_VALUE + 1; i < timestamp.getCounter(); i++) {
                record(new Timestamp(timestamp.getIdentifier(), i));
            }
        } else {
            final LinkedList<Interval> l = new LinkedList<Interval>();
            vv.put(timestamp.getIdentifier(), l);
            l.add(new Interval(Timestamp.MIN_VALUE + 1, timestamp.getCounter()));
        }
    }

    private void cleanUp(final Timestamp cc, LinkedList<Interval> l, ListIterator<Interval> it, Interval p) {
        if (p.from > p.to) {
            it.remove();
            numPairs--;
            if (l.isEmpty()) {
                drop(cc.getIdentifier());
            }
        }
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof VersionVectorWithExceptions)) {
            return false;
        }
        return this.compareTo((VersionVectorWithExceptions) other) == CausalityClock.CMP_CLOCK.CMP_EQUALS;
    }

    @Override
    public int hashCode() {
        //TODO : improve this hash
        int hash = 7;
        hash+=this.vv.hashCode();
        return hash;
    }
}