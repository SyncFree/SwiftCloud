package swift.crdt;

import swift.crdt.core.CRDTUpdate;

public class LowerBoundCounterIncrement implements CRDTUpdate<LowerBoundCounterCRDT> {

    private int amount;
    private String siteId;

    public LowerBoundCounterIncrement(String siteId, int amount) {
        this.amount = amount;
        this.siteId = siteId;
    }

    @Override
    public void applyTo(LowerBoundCounterCRDT crdt) {
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
