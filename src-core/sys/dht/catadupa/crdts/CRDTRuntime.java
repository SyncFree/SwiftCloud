package sys.dht.catadupa.crdts;

import sys.dht.catadupa.crdts.time.LVV;
import sys.dht.catadupa.crdts.time.Timestamp;

public class CRDTRuntime {

	protected LVV vv;
	protected String siteId;

	public CRDTRuntime(final String siteId) {
		this.siteId = siteId;
		vv = new LVV();
		CRDTRuntime = this;
	}

	public String siteId() {
		return siteId;
	}

	public LVV getCausalityClock() {
		return vv;
	}

	public void setSiteId(String siteId) {
		this.siteId = siteId;
	}

	public <V extends CvRDT<V>> Timestamp recordUpdate(CvRDT<V> object) {
		return vv.recordNext(siteId);
	}

	public static CRDTRuntime CRDTRuntime = null;
}
