package swift.crdt;

import swift.crdt.core.CRDTUpdate;

public class BoundedCounterDecrement<T extends BoundedCounterCRDT<T>> implements CRDTUpdate<T> {

    private int amount;
    private String siteId;

    public BoundedCounterDecrement() {

    }

    public BoundedCounterDecrement(String siteId, int amount) {
        this.amount = amount;
        this.siteId = siteId;
    }

    @Override
    public void applyTo(T crdt) {
        crdt.applyDec(this);
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

    @Override
    public Object getValueWithoutMetadata() {
        // TODO Auto-generated method stub
        return null;
    }
}
