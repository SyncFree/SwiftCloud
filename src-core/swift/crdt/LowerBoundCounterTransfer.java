package swift.crdt;

import swift.crdt.core.CRDTUpdate;

public class LowerBoundCounterTransfer implements CRDTUpdate<LowerBoundCounterCRDT> {

    private String originId, targetId;
    private int amount;

    public LowerBoundCounterTransfer(String originId, String targetId, int amount) {
        this.originId = originId;
        this.targetId = targetId;
        this.amount = amount;
    }

    @Override
    public void applyTo(LowerBoundCounterCRDT crdt) {
        crdt.applyTransfer(this);

    }

    protected String getOriginId() {
        return originId;
    }

    protected void setOriginId(String originId) {
        this.originId = originId;
    }

    protected String getTargetId() {
        return targetId;
    }

    protected void setTargetId(String targetId) {
        this.targetId = targetId;
    }

    protected int getAmount() {
        return amount;
    }

    protected void setAmount(int amount) {
        this.amount = amount;
    }

}
