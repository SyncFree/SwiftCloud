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
        if (table == null || key == null)
            throw new NullPointerException("uid cannot have null table or key");
        this.table = table;
        this.key = key;
    }

    /**
     * @return table for an object; never null
     */
    public String getTable() {
        return this.table;
    }

    /**
     * @return key for an object; never null
     */
    public String getKey() {
        return this.key;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        return prime * table.hashCode() + key.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CRDTIdentifier)) {
            return false;
        }
        CRDTIdentifier other = (CRDTIdentifier) obj;
        return table.equals(other.table) && key.equals(other.key);
    }
}
