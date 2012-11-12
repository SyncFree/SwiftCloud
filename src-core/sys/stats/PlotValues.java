package sys.stats;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import swift.utils.Pair;

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
