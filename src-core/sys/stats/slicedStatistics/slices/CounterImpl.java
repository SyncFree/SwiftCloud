package sys.stats.slicedStatistics.slices;

import sys.stats.slicedStatistics.SlicedStatistics;

public class CounterImpl implements SlicedStatistics<CounterImpl> {

    int counter;

    public CounterImpl() {

    }

    public void incCounter() {
        counter++;
    }

    public int getTotalOperations() {
        return counter;
    }

    public CounterImpl createNew() {
        return new CounterImpl();
    }

}
