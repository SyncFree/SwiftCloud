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
package sys.stats.slicedStatistics.slices.histogram;

import java.util.ArrayList;
import java.util.List;

import swift.utils.Pair;
import sys.stats.common.PlotValues;
import sys.stats.slicedStatistics.SlicedStatistics;

public class CommDistributionImpl implements Histogram, SlicedStatistics<CommDistributionImpl> {

    private List<Pair<Double, Integer>> countLessThan;
    private String sourceName;

    public CommDistributionImpl(String sourceName, double[] values) {
        this.sourceName = sourceName;
        this.countLessThan = new ArrayList<Pair<Double, Integer>>();
        for (double d : values) {
            countLessThan.add(new Pair<Double, Integer>(d, 0));
        }
    }

    public void addValue(double value) {
        for (Pair<Double, Integer> p : countLessThan) {
            if (p.getFirst() > value)
                p.setSecond(p.getSecond() + 1);
        }
    }

    @Override
    public CommDistributionImpl createNew() {
        double[] values = new double[countLessThan.size()];
        int i = 0;
        for (Pair<Double, Integer> v : countLessThan) {
            values[i] = v.getFirst();
            i++;
        }

        return new CommDistributionImpl(this.sourceName, values);
    }

    public PlotValues<Double, Integer> getValuesDistribution() {
        PlotValues<Double, Integer> values = new PlotValues<Double, Integer>();
        for (Pair<Double, Integer> cl : countLessThan) {
            values.addValue(cl.getFirst(), cl.getSecond());
        }
        return values;
    }

    @Override
    public PlotValues<Double, Integer> getHistogram() {
        PlotValues<Double, Integer> results = new PlotValues<Double, Integer>();
        for (Pair<Double, Integer> v : countLessThan)
            results.addValue(v.getFirst(), v.getSecond());
        return results;
    }
    
    public String toString(){
        return countLessThan.toString();
    }

}
