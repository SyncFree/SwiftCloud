/*
 * *********************************************************************
 * Copyright (c) 2010 Pedro Gomes and Universidade do Minho.
 * All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * ********************************************************************
 */

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.uminho.gsd.benchmarks.generic.entities;

import org.uminho.gsd.benchmarks.interfaces.Entity;

import java.sql.Date;
import java.util.TreeMap;

/**
 * O_ID O_C_ID O_DATE O_SUB_TOTAL O_TAX O_TOTAL O_SHIP_TYPE O_SHIP_DATE
 * O_BILL_ADDR_ID O_SHIP_ADDR_ID O_STATUS
 */
public class Order implements Entity {

	int O_ID;
	int O_C_ID; // Customer customer?
	Date O_DATE;
	double O_SUB_TOTAL;
	double O_TAX;
	double O_TOTAL;
	String O_SHIP_TYPE;
	Date O_SHIP_DATE;
	String O_STATUS;
	int billAddress;
	int O_SHIP_ADDR;

	public Order(int O_ID, int O_C_ID, Date O_DATE, double O_SUB_TOTAL,
			double O_TAX, double O_TOTAL, String O_SHIP_TYPE, Date shipDate,
			String O_STATUS, int billAddress, int O_SHIP_ADDR) {
		this.O_ID = O_ID;
		this.O_C_ID = O_C_ID;
		this.O_DATE = O_DATE;
		this.O_SUB_TOTAL = O_SUB_TOTAL;
		this.O_TAX = O_TAX;
		this.O_TOTAL = O_TOTAL;
		this.O_SHIP_TYPE = O_SHIP_TYPE;
		this.O_SHIP_DATE = shipDate;
		this.O_STATUS = O_STATUS;
		this.billAddress = billAddress;
		this.O_SHIP_ADDR = O_SHIP_ADDR;
	}

	public int getO_ID() {
		return O_ID;
	}

	public TreeMap<String, Object> getValuesToInsert() {

		// O_ID
		// O_C_ID
		// O_DATE
		// O_SUB_TOTAL
		// O_TAX
		// O_TOTAL
		// O_SHIP_TYPE
		// O_SHIP_DATE
		// O_BILL_ADDR_ID
		// O_SHIP_ADDR_ID
		// O_STATUS
		//

		TreeMap<String, Object> values = new TreeMap<String, Object>();

		values.put("O_C_ID", O_C_ID);
		values.put("O_DATE", O_DATE);
		values.put("O_SUB_TOTAL", O_SUB_TOTAL);
		values.put("O_TAX", O_TAX);
		values.put("O_TOTAL", O_TOTAL);

		values.put("O_SHIP_TYPE", O_SHIP_TYPE);
		values.put("O_SHIP_DATE", O_SHIP_DATE);

		values.put("O_BILL_ADDR_ID", billAddress);
		values.put("O_SHIP_ADDR_ID", O_SHIP_ADDR);

		values.put("O_STATUS", O_STATUS);

		return values;
	}

	public String getKeyName() {
		return "O_ID";
	}

	@Override
	public Object copy() {
		return new Order(O_ID, O_C_ID, O_DATE, O_SUB_TOTAL, O_TAX, O_TOTAL,
				O_SHIP_TYPE, O_SHIP_DATE, O_STATUS, billAddress, O_SHIP_ADDR);
	}
}