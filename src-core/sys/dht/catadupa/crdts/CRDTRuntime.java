/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
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
