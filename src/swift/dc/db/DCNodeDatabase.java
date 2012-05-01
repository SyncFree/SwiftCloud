package swift.dc.db;

import java.util.Properties;

import swift.crdt.CRDTIdentifier;
import swift.dc.CRDTData;

public interface DCNodeDatabase {

    boolean ramOnly();
    void init( Properties props);
    CRDTData<?> read( CRDTIdentifier id);
    boolean write(CRDTIdentifier id, CRDTData<?> data);
    Object readSysData(String table, String key);
    boolean writeSysData(String table, String key, Object data);
}
