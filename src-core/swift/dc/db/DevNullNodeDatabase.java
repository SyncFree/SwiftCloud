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

    @Override
    public boolean ramOnly() {
        return true;
    }

    @Override
    public Object readSysData(String table, String key) {
        return null;
    }

    @Override
    public boolean writeSysData(String table, String key, Object data) {
        return true;
    }
}
