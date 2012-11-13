package sys.stats.common;

public class PlaneValues<X, Y> {

    private X x;
    private Y y;

    public PlaneValues() {

    }

    public PlaneValues(X x, Y y) {
        super();
        this.x = x;
        this.y = y;
    }

    public X getX() {
        return x;
    }

    public void setX(X x) {
        this.x = x;
    }

    public Y getY() {
        return y;
    }

    public void setY(Y y) {
        this.y = y;
    }

    @Override
    public String toString() {
        return "(" + ((x != null) ? x.toString() : "NULL") + ";" + ((y != null) ? y.toString() : "NULL") + ")";
    }

}
