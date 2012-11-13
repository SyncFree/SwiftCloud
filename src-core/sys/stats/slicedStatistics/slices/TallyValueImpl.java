package sys.stats.slicedStatistics.slices;

import sys.stats.slicedStatistics.SlicedStatistics;
import umontreal.iro.lecuyer.stat.Tally;

public class TallyValueImpl implements SlicedStatistics<TallyValueImpl> {

    private Tally value;

    public TallyValueImpl() {
        value = new Tally();
    }

    public void addValue(double value) {
        this.value.add(value);
    }

    public int getTotalOperations() {
        return this.value.numberObs();
    }

    public double getSumValue() {
        return this.value.sum();
    }

    public double getAvgValue() {
        return this.value.average();
    }

    public TallyValueImpl createNew() {
        return new TallyValueImpl();
    }

}
