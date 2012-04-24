package swift.dc.db;

import java.util.Properties;

import swift.crdt.CRDTIdentifier;
import swift.dc.CRDTData;

/**
 * Database that stores no information
 * @author nmp
 *
 */
public class DevNullNodeDatabase implements DCNodeDatabase {

    @Override
    public void init(Properties props) {
        
    }

    @Override
    public CRDTData<?> read(CRDTIdentifier id) {
        return null;
    }

    @Override
    public boolean write(CRDTIdentifier id, CRDTData<?> data) {
        return true;
    }

}
