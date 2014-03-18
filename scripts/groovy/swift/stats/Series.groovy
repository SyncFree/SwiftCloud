package swift.stats

public class Series<X, Y> {

    final String name;
    final List<X> xVal = new ArrayList<X>();
    final List<Y> yVal = new ArrayList<Y>();
    final List<Y> eVal = new ArrayList<Y>();

    public Series(String name) {
        this.name = name;
    }

    public void add(X x, Y y) {
        xVal.add(x);
        yVal.add(y);
        eVal.add(0);
    }

    public void add(X x, Y y, Y e) {
        xVal.add(x);
        yVal.add(y);
        eVal.add(e);
    }

    public String name() {
        return name;
    }

    public int size() {
        return xVal.size();
    }

    public X xValue(int index) {
        return xVal.get(index);
    }

    public Y yValue(int index) {
        return yVal.get(index);
    }

    public Y eValue(int index) {
        return eVal.get(index);
    }
    public List<X> xValues() {
        return Collections.unmodifiableList(xVal);
    }

    public List<Y> yValues() {
        return Collections.unmodifiableList(yVal);
    }

    public List<Y> eValues() {
        return Collections.unmodifiableList(eVal);
    }

    public String toString() {
        return name();
    }
}
