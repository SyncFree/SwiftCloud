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

import java.util.ArrayList;
import java.util.List;

import swift.utils.Pair;
import sys.stats.common.PlotValues;
import sys.stats.slicedStatistics.SlicedStatistics;

public abstract class GenericStatisticsOverTime<V extends SlicedStatistics<V>> {

    private List<Pair<Long, V>> statisticsOverTime;
    private long sliceSize;
    protected long T0;

    protected GenericStatisticsOverTime(long sliceSize, V initialValue) {
        statisticsOverTime = new ArrayList<Pair<Long, V>>();
        this.sliceSize = sliceSize;
        this.T0 = System.currentTimeMillis();
        Pair<Long, V> newSlice = new Pair<Long, V>(0l, initialValue);
        statisticsOverTime.add(newSlice);
    }

    protected synchronized V getCurrentSlice() {
        long currentTimeMillis = System.currentTimeMillis() - T0;
        // System.out.println("T"+currentTimeMillis);
        Pair<Long, V> lastSlice = statisticsOverTime.get(statisticsOverTime.size() - 1);
        long lastTimeInterval = lastSlice.getFirst();
        if (currentTimeMillis >= lastTimeInterval) {
            long newSliceStart = (currentTimeMillis / sliceSize) * sliceSize + sliceSize;
            V newSliceValue = lastSlice.getSecond().createNew();
            Pair<Long, V> newSlice = new Pair<Long, V>(newSliceStart, newSliceValue);
            statisticsOverTime.add(newSlice);
        }
        return statisticsOverTime.get(statisticsOverTime.size() - 1).getSecond();

    }

    public void mergePreviousSlices(int limit) {
        // TODO
    }

    public int getSliceCount() {
        return statisticsOverTime.size();
    }

    public long getSliceSize() {
        return sliceSize;
    }

    public abstract PlotValues getPlotValues();

    protected List<Pair<Long, V>> getAllSlices() {
        return statisticsOverTime;
    }

    protected synchronized V addSliceAndReturn() {
        Pair<Long, V> lastSlice = statisticsOverTime.get(statisticsOverTime.size() - 1);
        this.statisticsOverTime.add(new Pair<Long, V>(System.currentTimeMillis() - T0, lastSlice.getSecond()
                .createNew()));
        return statisticsOverTime.get(statisticsOverTime.size() - 1).getSecond();

    }

}
