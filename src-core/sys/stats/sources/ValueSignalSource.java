package sys.stats.sources;

public interface ValueSignalSource {
    
    public void setValue(double value) ;

    public Stopper createEventDurationSignal();
    
    interface Stopper {
        void stop();
    }
}
