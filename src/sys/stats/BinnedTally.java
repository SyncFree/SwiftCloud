package sys.stats;

import java.util.ArrayList;
import java.util.List;

import umontreal.iro.lecuyer.stat.Tally;

public class BinnedTally {

	public String name ;
	public double binSize ;
	public List<Tally> bins  ;
	
	public BinnedTally(String name) {
		this( Double.MAX_VALUE, name ) ;
	}
	
	public BinnedTally(double binSize, String name) {
		this.name = name ;
		this.binSize = binSize;
		this.bins = new ArrayList<Tally>() ;
	}
	
	public void tally( double sample, double value ) {
		int i = (int)(sample / binSize) ;
		bin( i ).add(value) ;
	}
	
	public void tally( double sample, Tally t ) {
		int i = (int)(sample / binSize) ;
		while( i >= bins.size() )
			bins.add( new Tally() ) ;
		
		bins.set(i, t) ;
	}
	
	public Tally bin( int i) {
		while( i >= bins.size() )
			bins.add( new Tally() ) ;
		return bins.get(i) ;
	}
		
	public double totalObs() {
		double r = 0;
		for( Tally i : bins )
			r += i.numberObs() ;
		return r ;
	}
	
	public void init() {
		for( Tally i : bins )
			i.init() ;
	}
	
	public String name() {
		return name;
	}
	
	public BinnedTally ratio( BinnedTally other ) {
		BinnedTally res = new BinnedTally( binSize, name) ;
		for( int i = 0 ; i < bins.size() ; i++ ) {
			res.tally( i * binSize,  new WeirdRatioTally( bin(i), other.bin(i) ) ) ;
		}
		return res ;
	}
	
	public BinnedTally resample( int count ) {

		BinnedTally res = new BinnedTally( binSize * count, name ) ;

		int N = bins.size() ;
		for( int i = 0 ; i < bins.size() ; i+= count ) {
			int j = Math.min( N, i + count ) ;
			MultiTally mt = new MultiTally( bins.subList(i, j) ) ;
						
			res.tally(binSize * (j+1), mt ) ;
		}
		
		return res ;
	}
		
	static class MultiTally extends Tally {
		
		double num = 0, avg = 0, std = 0 ;
		MultiTally( List<Tally> tl ) {
			for( int i = 0 ; i < tl.size() ; i++ ) {
				Tally y = tl.get(i) ;
				if( y == null || y.numberObs() < 2 )
					continue ;
		
					double nx = num, sx = std, mx = avg ;
					double ny = y.numberObs(), sy = y.standardDeviation(), my = y.average() ;
	
					avg = (nx * mx + ny * my)/(nx + ny) ;
					std = Math.sqrt( ( nx * sx * sx + ny * sy * sy) / (nx+ny) + (nx*ny)* Math.pow(mx-my,2) / Math.pow(nx+ny,2)) ;
					sumValue += y.sum() ;
					num += y.numberObs() ;
					maxValue = Math.max( maxValue, y.max() ) ;
					minValue = Math.min( minValue, y.min() ) ;
				}
			}
		
		public double average() {
			return avg ;
		}
		
		public double standardDeviation() {
			return std ;
		}
		
		public int numberObs() {
			return (int)num;
		}
		
		public double sum() {
			return sumValue;
		}
	}
	
	static class WeirdRatioTally extends Tally {

		double avg, num ;
		
		WeirdRatioTally( Tally a, Tally b) {
			avg = a.average() / b.average() ;
			num = a.numberObs() ;
		}
		
		public double average() {
			return avg ;
		}
		
		public int numberObs() {
			return (int)num;
		}
	}
}