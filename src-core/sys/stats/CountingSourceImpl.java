package sys.stats;

import java.util.List;

import swift.utils.Pair;
import sys.stats.common.BinnedTally;

public class CountingSourceImpl extends AbstractTallyStatistics implements CountingSource {

    private long T0 = 0;

    public CountingSourceImpl(String sourceName, long frequencyMillis) {
        this.opsRecorder = new BinnedTally(frequencyMillis, sourceName);
        this.timespanMillis = frequencyMillis;
        this.sourceName = sourceName;
    }

    @Override
    public void incCounter() {
        if (T0 == 0) {
            T0 = System.currentTimeMillis();
        }
        this.opsRecorder.tally(System.currentTimeMillis() - T0, 1);
    }

    public int getTotalOperations() {
        return (int) opsRecorder.totalObs();
    }
    
    //TODO: Must test this
    public long getElapsedTimeMillis() {
        return ((long)opsRecorder.binSize) * opsRecorder.bins.size();
    }

    public List<Pair<Long, Double>> getCountOverTime(){
        return getSumOverTime(timespanMillis);
    }
    
    public List<Pair<Long, Double>> getCountOverTime(long frequencyMillis){
        return getSumOverTime(frequencyMillis);
    }


}
