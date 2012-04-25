package swift.clocks;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import swift.exceptions.IncompatibleTypeException;

/**
 * Class to represent version vectors with exceptions. This representation
 * records the intervals of contiguous values.
 * 
 * @author nmp
 */
public class VersionVectorWithExceptions implements CausalityClock {
    static class Pair {
        long from; // inclusive
        long to; // inclusive

        Pair() {
        }

        Pair(long from, long to) {
            this.from = from;
            this.to = to;
        }

        boolean includes(long l) {
            return l >= from && l <= to;
        }

        boolean mergeFwd(Pair p) {
            if (to == p.from + 1) {
                to = p.to;
                return true;
            } else
                return false;
        }

        boolean mergeBack(Pair p) {
            if (from == p.to + 1) {
                from = p.from;
                return true;
            } else
                return false;
        }

        Pair duplicate() {
            return new Pair(from, to);
        }
    }

    private static final long serialVersionUID = 1L;
    protected Map<String, LinkedList<Pair>> vv;
    protected int numPairs;

    public VersionVectorWithExceptions() {
        vv = new TreeMap<String, LinkedList<Pair>>();
        numPairs = 0;
    }

    protected VersionVectorWithExceptions(VersionVectorWithExceptions v) {
        vv = new TreeMap<String, LinkedList<Pair>>();
        numPairs = v.numPairs;
        Iterator<Entry<String, LinkedList<Pair>>> it = v.vv.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, LinkedList<Pair>> entry = it.next();
            String key = entry.getKey();
            LinkedList<Pair> l = entry.getValue();
            vv.put(key, duplicateList(l));
        }
    }

    protected VersionVectorWithExceptions(VersionVector v) {
        vv = new TreeMap<String, LinkedList<Pair>>();
        numPairs = 0;
        Iterator<Entry<String, Long>> it = v.vv.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Long> entry = it.next();
            String key = entry.getKey();
            LinkedList<Pair> nl = new LinkedList<Pair>();
            nl.add(new Pair(0, entry.getValue()));
            numPairs++;
            vv.put(key, nl);
        }
    }

    protected LinkedList<Pair> duplicateList(LinkedList<Pair> l) {
        LinkedList<Pair> nl = new LinkedList<Pair>();
        Iterator<Pair> it = l.iterator();
        while (it.hasNext()) {
            Pair p = it.next();
            nl.addLast(p.duplicate());
        }
        return nl;
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
        LinkedList<Pair> l = vv.get(cc.getIdentifier());
        if (l == null) {
            return false;
        }
        long v = cc.getCounter();
        ListIterator<Pair> it = l.listIterator(l.size());
        while (it.hasPrevious()) {
            Pair p = it.previous();
            if (v > p.to)
                return false;
            if (v >= p.from)
                return true;
        }
        return false;
    }

    /**
     * Records an event.
     * 
     * @param cc
     *            Timestamp to insert.
     */
    public boolean record(Timestamp cc) {
        long v = cc.getCounter();
        LinkedList<Pair> l = vv.get(cc.getIdentifier());
        if (l == null) {
            l = new LinkedList<Pair>();
            vv.put(cc.getIdentifier(), l);
            l.add(new Pair(v, v));
            return true;
        }
        ListIterator<Pair> it = l.listIterator(l.size());
        Pair p = null;
        while (it.hasPrevious()) {
            Pair oldP = p;
            p = it.previous();
            if( v >= p.from && v <= p.to)
                return true;
            if (v == p.to + 1) {
                p.to = p.to + 1;
                if (oldP != null && oldP.mergeBack(p)) {
                    it.remove();
                    numPairs--;
                }
                return true;
            } else if (v > p.to) {
                it.next();
                it.add(new Pair(v, v));
                numPairs++;
                return true;
            }
        }
        if (p != null) {
            if (p.from == v + 1) {
                p.from = v;
                return true;
            }
        }
        l.addFirst(new Pair(v, v));
        numPairs++;
        return true;
    }

    protected Pair advanceUntil(Pair p, Iterator<Pair> it, int val) {
        if (val <= p.to)
            return p;
        while (it.hasNext()) {
            p = it.next();
            if (val > p.to)
                continue;
            return p;
        }
        return null;
    }

/*    protected CMP_CLOCK mergeOneEntryVV(String siteid, LinkedList<Pair> l0) {
        LinkedList<Pair> l = vv.get(siteid);
        if (l == null) {
            l = duplicateList(l0);
            numPairs = numPairs + l0.size();
            vv.put(siteid, l);
            return CMP_CLOCK.CMP_ISDOMINATED;
        }
        boolean thisHasMoreEntries = false;
        boolean otherHasMoreEntries = false;
        LinkedList<Pair> nl = new LinkedList<Pair>();
        Iterator<Pair> it = l.iterator();
        Iterator<Pair> it0 = l0.iterator();
        Pair np = null;
        Pair p = it.hasNext() ? it.next() : null;
        Pair p0 = it0.hasNext() ? it0.next() : null;
        numPairs = 0;
        // last value that has been compared between the two sets
        long v = Math.min(p == null ? Long.MAX_VALUE : p.from - 1, p0 == null ? Long.MAX_VALUE : p0.from - 1);
        for (;;) {
            if (p == null && p0 == null)
                break;
            if (p != null && p0 != null) {
                
                
                if (p.from == p0.from && p.to == p0.to) {
                    nl.add(p);
                    numPairs++;
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
        vv.put(siteid, nl);

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
    
    
    
    protected CMP_CLOCK mergeOneEntryVV(String siteid, LinkedList<Pair> l0) {
        LinkedList<Pair> l = vv.get(siteid);
        if (l == null) {
            l = duplicateList(l0);
            numPairs = numPairs + l0.size();
            vv.put(siteid, l);
            return CMP_CLOCK.CMP_ISDOMINATED;
        }
        CMP_CLOCK cmp = compareOneEntryVV(siteid, l0);
        numPairs = numPairs - l.size();
        LinkedList<Pair> nl = new LinkedList<Pair>();
        Iterator<Pair> it = l.iterator();
        Iterator<Pair> it0 = l0.iterator();
        Pair p = it.hasNext() ? it.next() : null;
        Pair p0 = it0.hasNext() ? it0.next() : null;
        Pair np = null;
        for (;;) {
            boolean hasChanged = false;
            if (p == null && p0 == null)
                break;
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
                    if (p.to > np.to)
                        np.to = p.to;
                    p = null;
                    hasChanged = true;
                }
            }
            if (p0 != null) {
                if (np.to >= p0.from - 1) {
                    if (p0.to > np.to)
                        np.to = p0.to;
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
*/
    protected CMP_CLOCK mergeOneEntryVV(String siteid, LinkedList<Pair> l0) {
        LinkedList<Pair> l = vv.get(siteid);
        if (l == null) {
            l = duplicateList(l0);
            numPairs = numPairs + l0.size();
            vv.put(siteid, l);
            return CMP_CLOCK.CMP_ISDOMINATED;
        }
        CMP_CLOCK cmp = compareOneEntryVV(siteid, l0);
        numPairs = numPairs - l.size();
        LinkedList<Pair> nl = new LinkedList<Pair>();
        Iterator<Pair> it = l.iterator();
        Iterator<Pair> it0 = l0.iterator();
        Pair p = it.hasNext() ? it.next() : null;
        Pair p0 = it0.hasNext() ? it0.next() : null;
        Pair np = null;
        for (;;) {
            boolean hasChanged = false;
            if (p == null && p0 == null)
                break;
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
                    if (p.to > np.to)
                        np.to = p.to;
                    p = null;
                    hasChanged = true;
                }
            }
            if (p0 != null) {
                if (np.to >= p0.from - 1) {
                    if (p0.to > np.to)
                        np.to = p0.to;
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
        Iterator<Entry<String, LinkedList<Pair>>> it = cc.vv.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, LinkedList<Pair>> e = it.next();
            CMP_CLOCK partialResult = mergeOneEntryVV(e.getKey(), e.getValue());
            result = ClockUtils.combineCmpClock(result, partialResult);
        }
        it = vv.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, LinkedList<Pair>> e = it.next();
            LinkedList<Pair> l = cc.vv.get(e.getKey());
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
    public CMP_CLOCK merge(CausalityClock cc) {
        // if ( ! VersionVectorWithExceptions.class.equals(cc.getClass())) {
        // throw new IncompatibleTypeException();
        // }
        return mergeVV((VersionVectorWithExceptions) cc);
    }

    protected CMP_CLOCK compareOneEntryVV(String siteid, LinkedList<Pair> l0) {
        LinkedList<Pair> l = vv.get(siteid);
        if (l == null) {
            return CMP_CLOCK.CMP_ISDOMINATED;
        }

        boolean thisHasMoreEntries = false;
        boolean otherHasMoreEntries = false;
        Iterator<Pair> it = l.iterator();
        Iterator<Pair> it0 = l0.iterator();
        Pair p = it.hasNext() ? it.next() : null;
        Pair p0 = it0.hasNext() ? it0.next() : null;
        // last value that has been compared between the two sets
        long v = Math.min(p == null ? Long.MAX_VALUE : p.from - 1, p0 == null ? Long.MAX_VALUE : p0.from - 1);
        for (;;) {
            if (p == null && p0 == null)
                break;
            if (thisHasMoreEntries && otherHasMoreEntries)
                break;
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
        Iterator<Entry<String, LinkedList<Pair>>> it = cc.vv.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, LinkedList<Pair>> e = it.next();
            CMP_CLOCK partialResult = compareOneEntryVV(e.getKey(), e.getValue());
            result = ClockUtils.combineCmpClock(result, partialResult);
        }
        it = vv.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, LinkedList<Pair>> e = it.next();
            LinkedList<Pair> i = cc.vv.get(e.getKey());
            if (i == null) {
                result = ClockUtils.combineCmpClock(result, CMP_CLOCK.CMP_DOMINATES);
                break;
            }
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
     * @param siteid
     *            Site identifier.
     * @return Returns an event clock.
     */
    public Timestamp getLatest(String siteid) {
        LinkedList<Pair> p = vv.get(siteid);
        if (p == null) {
            return new Timestamp(siteid, Timestamp.MIN_VALUE);
        } else {
            return new Timestamp(siteid, p.getLast().to);
        }
    }

    /**
     * Returns the most recent event for a given site. <br>
     * 
     * @param siteid
     *            Site identifier.
     * @return Returns an event clock.
     */
    public long getLatestCounter(String siteid) {
        LinkedList<Pair> p = vv.get(siteid);
        if (p == null) {
            return Timestamp.MIN_VALUE;
        } else {
            return p.getLast().to;
        }
    }

    @Override
    public boolean hasEventFrom(String siteid) {
        return getLatestCounter(siteid) != Timestamp.MIN_VALUE;
    }

    /**
     * Create a copy of this causality clock.
     */
    public CausalityClock clone() {
        return new VersionVectorWithExceptions(this);
    }

    public String toString() {
        StringBuffer buf = new StringBuffer();
        buf.append("[");
        Iterator<Entry<String, LinkedList<Pair>>> it = vv.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, LinkedList<Pair>> e = it.next();
            buf.append(e.getKey() + ":");
            Iterator<Pair> it2 = e.getValue().iterator();
            while (it2.hasNext()) {
                Pair p = it2.next();
                buf.append("[");
                buf.append(p.from);
                buf.append("-");
                buf.append(p.to);
                buf.append("]");
            }
            if (it.hasNext()) {
                buf.append(",");
            }
        }
        buf.append("]");
        return buf.toString();
    }

    public boolean hasExceptions() {
        return vv.size() != numPairs;
    }

    @Override
    public void drop(String siteId) {
        vv.remove(siteId);
    }

    @Override
    public void drop(final Timestamp cc) {
        LinkedList<Pair> l = vv.get(cc.getIdentifier());
        if (l == null) {
            return;
        }
        long v = cc.getCounter();
        ListIterator<Pair> it = l.listIterator(l.size());
        Pair p = null;
        while (it.hasPrevious()) {
            Pair oldP = p;
            p = it.previous();
            if (v > p.to) {
                return;
            } else if (v == p.to) {
                p.to = p.to - 1;
                if (p.from > p.to) {
                    it.remove();
                    numPairs--;
                    if (l.size() == 0)
                        vv.remove(cc.getIdentifier());
                }
                return;
            } else if (v == p.from) {
                p.from = p.from + 1;
                if (p.from > p.to) {
                    it.remove();
                    numPairs--;
                    if (l.size() == 0)
                        vv.remove(cc.getIdentifier());
                }
                return;
            } else if (v > p.from && v < p.to) {
                p.from = v + 1;
                it.add(new Pair(p.from, v - 1));
                numPairs++;
                return;
            }
        }
    }
}
