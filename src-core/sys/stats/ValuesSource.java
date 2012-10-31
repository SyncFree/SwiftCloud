package sys.stats;

public interface ValuesSource {
    void recordEventWithValue(double value);
    Stopper createEventRecordingStopper();

    interface Stopper {
        void stop();
    }
}
