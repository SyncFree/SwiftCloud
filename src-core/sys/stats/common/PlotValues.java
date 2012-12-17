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
package sys.stats.common;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PlotValues<X, Y> {

    List<PlaneValues<X, Y>> values;

    public PlotValues() {
        values = new ArrayList<PlaneValues<X, Y>>();
    }

    public PlotValues(List<PlaneValues<X, Y>> slices) {
        this.values = slices;

    }

    public void addValue(X x, Y y) {
        values.add(new PlaneValues<X, Y>(x, y));
    }

    public Iterator<PlaneValues<X, Y>> getPlotValuesIterator() {
        return values.iterator();
    }

    @Override
    public String toString() {
        return values.toString();
    }

}
