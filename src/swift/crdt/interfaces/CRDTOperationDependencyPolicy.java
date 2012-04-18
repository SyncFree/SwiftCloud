package swift.crdt.interfaces;

/**
 * Policy defining how to treat operation group dependencies when attempting to
 * execute operation group on a CRDT object.
 * 
 * @author mzawirski
 */
public enum CRDTOperationDependencyPolicy {
    /**
     * Check operation group dependencies when trying to execute it, fail if
     * they are not me.
     */
    CHECK,
    /**
     * Do not check operation group dependencies when trying to execute it,
     * assume they are met and record dependencies clock as if they were already
     * included in the state.
     */
    RECORD_BLINDLY,
    /**
     * Do not check operation group dependencies when trying to execute it.
     */
    IGNORE
}