package sys.stats.common;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class Stats {

	
	private static Map<String, Map<String, Histogram<Integer>>> histograms = new HashMap<String, Map<String, Histogram<Integer>>>();
	
	public static  Histogram<Integer> histogram(String scenario, String name) {
		Map<String, Histogram<Integer>> stm = histograms.get( scenario ) ;
		if( stm == null )
			histograms.put(scenario, stm = new HashMap<String, Histogram<Integer>>());

		Histogram<Integer> res = stm.get( name ) ;
		if( res == null )
			stm.put( name, res = new Histogram<Integer>( name ) ) ;

		return res;
	}

	static public Collection<String> keys() {
		return histograms.keySet();
	}
	
	static public Collection<Histogram<Integer>> histograms( String scenario ) {
		Map<String, Histogram<Integer>> tmp = histograms.get( scenario) ;
		return tmp != null ? tmp.values() : new ArrayList<Histogram<Integer>>();
	}
	
//	static double getMax( String scenario) {
//		double res = -Double.MAX_VALUE;
//		for (BinnedTally i : tallies.get(scenario).values())
//			res = Math.max(res, i.binSize * i.bins.size());
//
//		return res;
//	}

}
