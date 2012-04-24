package swift.clocks;

/**
 * Using the ClockFactory and hiding all the actual constructors of the
 * different causality clocks, we enforce that there is only one kind of version
 * vector used.
 * 
 * For now, we only use VersionVectorWithExceptions!
 * 
 * TODO Make the choice for the kind of version vector part of the configuration
 * file.
 * 
 * @author annettebieniusa
 * 
 */
public class ClockFactory {
    public static CausalityClock newClock() {
        return new VersionVectorWithExceptions();
    }

    public static CausalityClock newClock(CausalityClock c) {
        if (c instanceof VersionVectorWithExceptions) {
            return new VersionVectorWithExceptions((VersionVectorWithExceptions) c);
        } else {
            throw new RuntimeException(
                    "This should not have happened! The system only uses VersionVectorWithExceptions!");
        }

    }

}