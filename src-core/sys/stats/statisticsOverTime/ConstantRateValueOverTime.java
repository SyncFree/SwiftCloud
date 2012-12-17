/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package sys.stats.statisticsOverTime;

import java.util.List;

import swift.utils.Pair;
import sys.stats.common.PlotValues;
import sys.stats.slicedStatistics.slices.ValueImpl;
import sys.stats.sources.ValueSignalSource;

public class ConstantRateValueOverTime extends GenericStatisticsOverTime<ValueImpl> implements ValueSignalSource {

    public ConstantRateValueOverTime(long timeSlice, String sourceName) {
        super(timeSlice, new ValueImpl());
    }

    @Override
    public synchronized void setValue(double value) {
        long currTime = System.currentTimeMillis() - T0;
        List<Pair<Long, ValueImpl>> allSlices = getAllSlices();
        if (allSlices.get(allSlices.size() - 1).getFirst() - currTime == 0)
            allSlices.get(allSlices.size() - 1).getSecond().setValue(value);
        else {
            ValueImpl slice = addSliceAndReturn();
            slice.setValue(value);
        }

    }

    @Override
    public Stopper createEventDurationSignal() {
        final long TS = System.currentTimeMillis();
        Stopper stopper = new Stopper() {
            public void stop() {
                long TE = System.currentTimeMillis();
                long duration = TE - TS;
                setValue(duration);
            }

        };
        return stopper;

    }

    @Override
    public synchronized PlotValues<Long, Double> getPlotValues() {
        List<Pair<Long, ValueImpl>> slices = getAllSlices();
        PlotValues<Long, Double> plotValues = new PlotValues<Long, Double>();
        for (Pair<Long, ValueImpl> v : slices) {
            plotValues.addValue(v.getFirst(), v.getSecond().getValue());
        }
        return plotValues;
    }

}
