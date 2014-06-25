package swift.utils;

public interface FutureResultHandler<V> {
    public void onResult(final V result);
}
