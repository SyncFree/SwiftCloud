package swift.dc.db;

import java.util.Properties;

import swift.crdt.CRDTIdentifier;
import swift.dc.CRDTData;

public interface DCNodeDatabase {

    void init( Properties props);
    CRDTData<?> read( CRDTIdentifier id);
    boolean write(CRDTIdentifier id, CRDTData<?> data);
}
