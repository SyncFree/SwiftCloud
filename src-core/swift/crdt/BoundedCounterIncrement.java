package swift.crdt;

import swift.crdt.core.CRDTUpdate;

public class BoundedCounterIncrement<T extends BoundedCounterCRDT<T>> implements CRDTUpdate<T> {

    private int amount;
    private String siteId;

    public BoundedCounterIncrement() {

    }

    public BoundedCounterIncrement(String siteId, int amount) {
        this.amount = amount;
        this.siteId = siteId;
    }

    @Override
    public void applyTo(T crdt) {
        crdt.applyInc(this);
    }

    protected int getAmount() {
        return amount;
    }

    protected void setAmount(int amount) {
        this.amount = amount;
    }

    protected String getSiteId() {
        return siteId;
    }

    protected void setSiteId(String siteId) {
        this.siteId = siteId;
    }

}
