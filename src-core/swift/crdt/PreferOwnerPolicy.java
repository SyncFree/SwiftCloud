package swift.crdt;

import java.util.ArrayList;
import java.util.List;

public class PreferOwnerPolicy implements PreferenceListPolicy {

    @Override
    public List<String> getPreferenceList(SharedLockCRDT lock) {
        String primaryOwner = lock.getPrimaryOwner();
        ArrayList<String> prefrredOwners = new ArrayList<String>(lock.getCurrentOwners());
        prefrredOwners.add(0, primaryOwner);
        return prefrredOwners;
    }

}
