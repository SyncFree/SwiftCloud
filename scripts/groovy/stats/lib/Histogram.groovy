

import java.util.Collection;
import java.util.TreeMap;
import umontreal.iro.lecuyer.stat.Tally;
@SuppressWarnings("serial")
public class Histogram<V extends Number & Comparable<V>> {

	public String name;
	public double binSize;
	public TreeMap<Double, V> categories = new TreeMap<Double, V>();
	public TreeMap<Double, Tally> tallies = new TreeMap<Double, Tally>();

    public Tally accum = new Tally("accum");
    
	public Histogram() {
		this("", 0);
	}

	public Histogram(String name) {
		this(name, 0);
	}

	public Histogram(String name, double binSize) {
		this.name = name;
		this.binSize = binSize;
	}

	public String name() {
		return name;
	}

	public V firstCategory() {
		return categories.firstEntry().getValue();
	}

	public V lastCategory() {
		return categories.lastEntry().getValue();
	}

	public Tally getTally(V v, boolean create) {
        accum.add( v.doubleValue() ) ;
		double x = binSize == 0 ? v.doubleValue() : ((long) (v.doubleValue() / binSize)) * binSize;
		Tally t = tallies.get(x);
		if (t == null && create) {
			categories.put(x, v);
			tallies.put(x, t = new Tally(name + "-" + v));
		}
		return t;
	}

	public void tally(V v, double y) {
		getTally(v, true).add(y);
	}

	public int totalObs() {
		int res = 0;
		for (Tally i : tallies.values())
			res += i.numberObs();

		return res;
	}

	public Collection<V> categories() {
		return categories.values();
	}

	public Collection<Tally> values() {
		return tallies.values();
	}

	public int size() {
		return categories.size();
	}

	public Number[] xValues() {
		int i = 0;
		Number[] res = new Number[size()];
		for (Double x : categories.keySet())
			res[i++] = x;
		return res;
	}

	public Tally[] yValues() {
		return tallies.values().toArray(new Tally[size()]);
	}

	public String toString() {
		return tallies.values().toString();
	}

	Series<V, Tally> toSeries(String name) {
		Series<V, Tally> res = new Series<V, Tally>(name);
		for (Double v : tallies.keySet()) {
			res.add(categories.get(v), tallies.get(v));
		}
		return res;
	}
    
    public Tally aggregated() {
        return accum;
    }
}
