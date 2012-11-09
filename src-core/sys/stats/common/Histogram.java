package sys.stats.common;

import java.util.Map;
import java.util.TreeMap;
import umontreal.iro.lecuyer.stat.Tally;

@SuppressWarnings("serial")
public class Histogram<V extends Comparable<V>> extends TreeMap<V, Tally> {
	
	public String name;
	
	Histogram( String name ) {
		this.name = name;
	}
	
	public String name() {
		return name;
	}
	
	public void tally( V v, double y ) {
		Tally t = super.get( v);
		if( t == null )
			super.put( v, t = new Tally(name+"-" + v) ) ;
		
		t.add( y ) ;
	}
	
	public int totalObs() {
		int res = 0;
		for( Tally i : super.values() )
			res += i.numberObs();

		return res;
	}
	
	public BinnedTally toBinnedTally( String name, double binSize ) {
		BinnedTally res = new BinnedTally(name);
		for( Map.Entry<V, Tally> i : entrySet() ) {
			Number x = (Number)i.getKey();
			Tally t = i.getValue();
			res.tally( x.doubleValue(), t.sum() );
		}
		return res;
	}
	
	public double[] xValues() {
		double[] res = new double[ size() ] ;
		int j = 0;
		for( Map.Entry<V, Tally> i : entrySet() ) {
			Number x = (Number)i.getKey();
			res[j++] = x.doubleValue();
		}
		return res;
	}

	public Tally[] yValues() {
		Tally[] res = new Tally[ size() ] ;
		int j = 0;
		for( Map.Entry<V, Tally> i : entrySet() ) {
			res[j++] = i.getValue();
		}
		return res;
	}

}
