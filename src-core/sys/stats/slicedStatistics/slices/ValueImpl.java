package sys.stats.slicedStatistics.slices;

import sys.stats.slicedStatistics.SlicedStatistics;

public class ValueImpl implements SlicedStatistics<ValueImpl> {

    private double value;

    public ValueImpl() {

    }

    public void setValue(double value) {
        this.value = value;
    }

    public double getValue() {
        return value;
    }

    public ValueImpl createNew() {
        return new ValueImpl();
    }

}
