package swift.crdt;

import java.util.List;

public interface PreferenceListPolicy {

    public List<String> getPreferenceList(SharedLockCRDT lock);

}
