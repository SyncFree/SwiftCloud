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

public class CCXact implements Entity {

	long cx_id;
	String type;
	long num;
	String name;
	Date expiry;
	/* String authId; */
	double total;
	Date shipDate;
	long order;
	int country;

	public CCXact(String type, long num, String name, Date expiry,
			double total, Date shipDate, long order, int country) {

		this.cx_id = order;
		this.type = type;
		this.num = num;
		this.name = name;
		this.expiry = expiry;
		this.total = total;
		this.shipDate = shipDate;
		this.order = order;
		this.country = country;
	}

	public int getCountry() {
		return country;
	}

	public void setCountry(int country) {
		this.country = country;
	}

	public Date getExpiry() {
		return expiry;
	}

	public void setExpiry(Date expiry) {
		this.expiry = expiry;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public long getNum() {
		return num;
	}

	public void setNum(long num) {
		this.num = num;
	}

	public long getOrder() {
		return order;
	}

	public void setOrder(long order) {
		this.order = order;
	}

	public Date getShipDate() {
		return shipDate;
	}

	public void setShipDate(Date shipDate) {
		this.shipDate = shipDate;
	}

	public double getTotal() {
		return total;
	}

	public void setTotal(double total) {
		this.total = total;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public TreeMap<String, Object> getValuesToInsert() {
		TreeMap<String, Object> values = new TreeMap<String, Object>();

		values.put("CX_TYPE", type);
		values.put("CX_NUM", num);
		values.put("CX_NAME", name);
		values.put("CX_EXPIRY", expiry);
		values.put("CX_XACT_DATE", shipDate);
		values.put("CX_XACT_AMT", total);
		values.put("CX_CO_ID", country);

		return values;
	}

	public String getKeyName() {
		return "CX_O_ID";
	}

	@Override
	public Object copy() {
		return new CCXact(type, num, name, expiry, total, shipDate, order,
				country);
	}

}