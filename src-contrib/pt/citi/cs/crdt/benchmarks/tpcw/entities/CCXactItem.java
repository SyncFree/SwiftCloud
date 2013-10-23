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
package pt.citi.cs.crdt.benchmarks.tpcw.entities;

import org.uminho.gsd.benchmarks.interfaces.Entity;
import java.util.TreeMap;

public class CCXactItem implements Entity {

	public String CX_O_ID;
	public String CX_TYPE;
	public long CX_CC_NUM;
	public String CX_CC_NAME;
	public long CX_CC_EXPIRY;
	/* String authId; */
	public double CX_XACT_AMT;
	public long CX_XACT_DATE;
	public int CX_CO_ID;

	CCXactItem() {
	}

	public CCXactItem(String type, long num, String name, long expiry,
			double total, long shipDate, String order, int country) {

		this.CX_O_ID = order;
		this.CX_TYPE = type;
		this.CX_CC_NUM = num;
		this.CX_CC_NAME = name;
		this.CX_CC_EXPIRY = expiry;
		this.CX_XACT_AMT = total;
		this.CX_XACT_DATE = shipDate;
		this.CX_CO_ID = country;
	}

	public String getCX_O_ID() {
		return CX_O_ID;
	}

	public void setCX_O_ID(String cx_id) {
		this.CX_O_ID = cx_id;
	}

	public int getCountry() {
		return CX_CO_ID;
	}

	public void setCountry(int country) {
		this.CX_CO_ID = country;
	}

	public long getExpiry() {
		return CX_CC_EXPIRY;
	}

	public void setExpiry(long expiry) {
		this.CX_CC_EXPIRY = expiry;
	}

	public String getName() {
		return CX_CC_NAME;
	}

	public void setName(String name) {
		this.CX_CC_NAME = name;
	}

	public long getNum() {
		return CX_CC_NUM;
	}

	public void setNum(long num) {
		this.CX_CC_NUM = num;
	}

	public long getShipDate() {
		return CX_XACT_DATE;
	}

	public void setShipDate(long shipDate) {
		this.CX_XACT_DATE = shipDate;
	}

	public double getTotal() {
		return CX_XACT_AMT;
	}

	public void setTotal(double total) {
		this.CX_XACT_AMT = total;
	}

	public String getType() {
		return CX_TYPE;
	}

	public void setType(String type) {
		this.CX_TYPE = type;
	}
	public TreeMap<String, Object> getValuesToInsert() {
		TreeMap<String, Object> values = new TreeMap<String, Object>();

		values.put("CX_TYPE", CX_TYPE);
		values.put("CX_CC_NUM", CX_CC_NUM);
		values.put("CX_CC_NAME", CX_CC_NAME);
		values.put("CX_CC_EXPIRY", CX_CC_EXPIRY);
		values.put("CX_XACT_DATE", CX_XACT_DATE);
		values.put("CX_XACT_AMT", CX_XACT_AMT);
		values.put("CX_CO_ID", CX_CO_ID);
		values.put("CX_O_ID", CX_O_ID);

		return values;
	}

	public String getKeyName() {
		return "CX_O_ID";
	}

	@Override
	public Object copy() {
		return new CCXactItem(CX_TYPE, CX_CC_NUM, CX_CC_NAME, CX_CC_EXPIRY,
				CX_XACT_AMT, CX_XACT_DATE, CX_O_ID, CX_CO_ID);
	}

}