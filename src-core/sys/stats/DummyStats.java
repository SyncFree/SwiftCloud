package sys.stats;

import java.io.IOException;

import sys.stats.sources.CounterSignalSource;
import sys.stats.sources.PollingBasedValueProvider;
import sys.stats.sources.ValueSignalSource;

/**
 * Dummy {@link Stats} implementation that does not record anything but
 * maintains interface compability with the actual implementation
 * {@link StatsImpl}.
 * 
 * @author mzawirsk
 */
public class DummyStats implements Stats {

    @Override
    public void registerPollingBasedValueProvider(String statName, PollingBasedValueProvider provider, int frequency) {
    }

    @Override
    public ValueSignalSource getValuesFrequencyOverTime(String statName, double... valueBins) {
        return new ValueSignalSource() {
            @Override
            public void setValue(double value) {
            }

            @Override
            public Stopper createEventDurationSignal() {
                return new Stopper() {
                    @Override
                    public void stop() {
                    }
                };
            }
        };
    }

    @Override
    public CounterSignalSource getCountingSourceForStat(String statName) {
        return new CounterSignalSource() {
            @Override
            public void incCounter() {
            }

            @Override
            public void decCounter() {
            }
        };
    }

    @Override
    public void outputAndDispose() throws IOException {
    }
}
