package swift.crdt;

import swift.crdt.core.CRDTUpdate;

public class LowerBoundCounterDecrement implements CRDTUpdate<LowerBoundCounterCRDT> {

    private int amount;
    private String siteId;

    public LowerBoundCounterDecrement(String siteId, int amount) {
        this.amount = amount;
        this.siteId = siteId;
    }

    @Override
    public void applyTo(LowerBoundCounterCRDT crdt) {
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

}
