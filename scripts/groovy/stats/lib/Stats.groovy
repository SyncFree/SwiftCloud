import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class Stats<T extends Number & Comparable<T>> {

	private Map<String, Map<String, Histogram<T>>> histograms = new HashMap<String, Map<String, Histogram<T>>>();
	private Map<String, Map<String, Series<T, Double>>> series = new HashMap<String, Map<String, Series<T, Double>>>();

	public Histogram<T> histogram(String groupKey, String key) {
		Map<String, Histogram<T>> stm = histograms.get(groupKey);
		if (stm == null)
			histograms.put(groupKey, stm = new HashMap<String, Histogram<T>>());

		Histogram<T> res = stm.get(key);
		if (res == null)
			stm.put(key, res = new Histogram<T>(key));

		return res;
	}

	public Histogram<T> histogram(String groupKey, String key, int binSize) {
		Map<String, Histogram<T>> stm = histograms.get(groupKey);
		if (stm == null)
			histograms.put(groupKey, stm = new HashMap<String, Histogram<T>>());

		Histogram<T> res = stm.get(key);
		if (res == null)
			stm.put(key, res = new Histogram<T>(key, binSize));

		return res;
	}

	public Series<T, Double> series(String groupKey, String key) {
		Map<String, Series<T, Double>> stm = series.get(groupKey);
		if (stm == null)
			series.put(groupKey, stm = new HashMap<String, Series<T, Double>>());

		Series<T, Double> res = stm.get(key);
		if (res == null)
			stm.put(key, res = new Series<T, Double>(key));

		return res;
	}

	public Collection<String> keys() {
		return histograms.keySet();
	}

	public Collection<Histogram<T>> histograms(String groupKey) {
		Map<String, Histogram<T>> tmp = histograms.get(groupKey);
		return tmp != null ? tmp.values() : new ArrayList<Histogram<T>>();
	}

	public Collection<Series<T, Double>> series(String groupKey) {
		Map<String, Series<T, Double>> tmp = series.get(groupKey);
		return tmp != null ? tmp.values() : new ArrayList<Series<T, Double>>();
	}
}
