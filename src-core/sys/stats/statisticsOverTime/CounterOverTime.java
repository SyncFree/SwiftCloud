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
import sys.stats.slicedStatistics.slices.CounterImpl;
import sys.stats.sources.CounterSignalSource;

public class CounterOverTime extends GenericStatisticsOverTime<CounterImpl> implements CounterSignalSource {

    public CounterOverTime(long timeSlice, String sourceName) {
        super(timeSlice, new CounterImpl());
    }

    @Override
    public void incCounter() {
        CounterImpl slice = getCurrentSlice();
        slice.incCounter();
    }

    public int getTotalCount() {
        int count = 0;
        List<Pair<Long, CounterImpl>> slices = getAllSlices();
        for (Pair<Long, CounterImpl> s : slices) {
            count += s.getSecond().getTotalOperations();
        }
        return count;
    }

    @Override
    public PlotValues<Long, Integer> getPlotValues() {
        List<Pair<Long, CounterImpl>> slices = getAllSlices();
        PlotValues<Long, Integer> plotValues = new PlotValues<Long, Integer>();
        for (Pair<Long, CounterImpl> v : slices) {
            plotValues.addValue(v.getFirst(), v.getSecond().getTotalOperations());
        }
        return plotValues;
    }

}
