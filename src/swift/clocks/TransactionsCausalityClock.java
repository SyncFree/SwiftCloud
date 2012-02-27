package swift.clocks;

import swift.exceptions.InvalidParameterException;

/**
 * Base class for clocks that allow to trace causality, such as version vector
 * and dotted version vectors
 * 
 * @author nmp
 * @param <T>
 *            EventClock type
 * @deprecated logic moved to TransactionHandlerImpl
 */
@Deprecated
public interface TransactionsCausalityClock<T extends Timestamp> {
    void initTransaction(String serverId);

    T recordNextAtClient();

    void commitTransaction(T serverCommitEvent)
            throws InvalidParameterException;
}
