package swift.crdt;

/**
 * System-wide unique identifier for CRDT object. Identification via table to
 * which the CRDT is associated and key under which the CRDT is stored.
 * 
 * @author annettebieniusa
 * 
 */
// TODO: provide custom serializer or Kryo-lize the class
public class CRDTIdentifier {
    private final String table;
    private final String key;
    
    public CRDTIdentifier(String table, String key) {
        this.table = table;
        this.key = key;
    }

    public String getTable() {
        return this.table;
    }

    public String getKey() {
        return this.key;
    }
}
