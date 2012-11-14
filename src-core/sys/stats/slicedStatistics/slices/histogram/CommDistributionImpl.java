package sys.stats.slicedStatistics.slices.histogram;

import java.util.ArrayList;
import java.util.List;

import swift.utils.Pair;
import sys.stats.common.PlotValues;
import sys.stats.slicedStatistics.SlicedStatistics;

public class CommDistributionImpl implements Histogram, SlicedStatistics<CommDistributionImpl> {

    private List<Pair<Double, Integer>> countLessThan;
    private String sourceName;

    public CommDistributionImpl(String sourceName, double[] values) {
        this.sourceName = sourceName;
        this.countLessThan = new ArrayList<Pair<Double, Integer>>();
        for (double d : values) {
            countLessThan.add(new Pair<Double, Integer>(d, 0));
        }
    }

    public void addValue(double value) {
        for (Pair<Double, Integer> p : countLessThan) {
            if (p.getFirst() > value)
                p.setSecond(p.getSecond() + 1);
        }
    }

    @Override
    public CommDistributionImpl createNew() {
        double[] values = new double[countLessThan.size()];
        int i = 0;
        for (Pair<Double, Integer> v : countLessThan) {
            values[i] = v.getFirst();
            i++;
        }

        return new CommDistributionImpl(this.sourceName, values);
    }

    public PlotValues<Double, Integer> getValuesDistribution() {
        PlotValues<Double, Integer> values = new PlotValues<Double, Integer>();
        for (Pair<Double, Integer> cl : countLessThan) {
            values.addValue(cl.getFirst(), cl.getSecond());
        }
        return values;
    }

    @Override
    public PlotValues<Double, Integer> getHistogram() {
        PlotValues<Double, Integer> results = new PlotValues<Double, Integer>();
        for (Pair<Double, Integer> v : countLessThan)
            results.addValue(v.getFirst(), v.getSecond());
        return results;
    }
    
    public String toString(){
        return countLessThan.toString();
    }

}
