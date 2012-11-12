package sys.stats;

public interface ValuesSignal {
    
    public void recordSignal(double value);

    public Stopper createEventDurationSignal();
    
    interface Stopper {
        void stop();
    }
}
