package sys.stats;

import java.util.LinkedList;
import java.util.List;

import swift.utils.Pair;
import sys.stats.common.BinnedTally;
import umontreal.iro.lecuyer.stat.Tally;

public abstract class AbstractTallyStatistics {

    protected long timespanMillis;
    protected String sourceName;
    protected BinnedTally opsRecorder;

    private List<Pair<Long, Double>> getSumOverTime(BinnedTally bt, double timespanMillis) {
        LinkedList<Pair<Long, Double>> results = new LinkedList<Pair<Long, Double>>();
        long dT = 0;
        for (Tally t : bt.bins) {
            dT += timespanMillis;
            if (t.numberObs() != 0)
                results.add(new Pair<Long, Double>(dT, t.sum()));
        }
        return results;

    }

    protected List<Pair<Long, Double>> getSumOverTime(double timespanMillis) {
        assert timespanMillis > this.timespanMillis;

        BinnedTally newBinnedTally = new BinnedTally(timespanMillis, sourceName);
        long dT = 0;
        for (Tally t : this.opsRecorder.bins) {
            if (t.numberObs() != 0)
                newBinnedTally.tally(dT, t.sum());
            dT += this.timespanMillis;
        }
        return getSumOverTime(newBinnedTally, timespanMillis);

    }

    protected List<Pair<Long, Double>> getSumOverTime() {
        return getSumOverTime(opsRecorder, timespanMillis);

    }

    private List<Pair<Long, Double>> getAverageOverTime(BinnedTally bt, long timespanMillis) {
        LinkedList<Pair<Long, Double>> results = new LinkedList<Pair<Long, Double>>();
        long dT = 0;
        for (Tally t : bt.bins) {
            dT += timespanMillis;
            if (t.numberObs() != 0)
                results.add(new Pair<Long, Double>(dT, t.average()));
        }
        return results;

    }

    protected List<Pair<Long, Double>> getAverageOverTime() {
        return getAverageOverTime(opsRecorder, timespanMillis);

    }

}
