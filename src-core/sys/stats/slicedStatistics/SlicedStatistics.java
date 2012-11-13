package sys.stats.slicedStatistics;

public interface SlicedStatistics<V extends SlicedStatistics<V>> {
    public V createNew();
}
