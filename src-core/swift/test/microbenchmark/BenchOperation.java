package swift.test.microbenchmark;

import swift.crdt.CRDTIdentifier;

public class BenchOperation<V> {
    private CRDTIdentifier identifier;
    private V value;
    private OpType  type;
    
    public BenchOperation(CRDTIdentifier identifier, V value, OpType type) {
        super();
        this.identifier = identifier;
        this.value = value;
        this.type = type;
    }

    public CRDTIdentifier getIdentifier() {
        return identifier;
    }

    public V getValue() {
        return value;
    }

    public OpType getType() {
        return type;
    }
    
    
    
    
}
