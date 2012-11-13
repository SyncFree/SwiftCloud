package sys.stats.slicedStatistics.slices.histogram;

import sys.stats.common.PlotValues;

public interface Histogram {

    PlotValues<Double, Integer> getHistogram();

}
