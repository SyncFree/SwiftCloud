package swift.crdt;

import swift.crdt.core.CRDTUpdate;

public class AcquireLockUpdate implements CRDTUpdate<SharedLockCRDT> {

    private String ownerId;
    private LockType type;

    public AcquireLockUpdate(String ownerId, LockType type) {
        this.ownerId = ownerId;
        this.type = type;
    }

    @Override
    public void applyTo(SharedLockCRDT crdt) {
        crdt.applyAcquireLock(this);
    }

    protected String getOwnerId() {
        return ownerId;
    }

    protected void setOwnerId(String ownerId) {
        this.ownerId = ownerId;
    }

    protected LockType getType() {
        return type;
    }

    protected void setType(LockType type) {
        this.type = type;
    }

    @Override
    public Object getValueWithoutMetadata() {
        // TODO Auto-generated method stub
        return null;
    }
}
