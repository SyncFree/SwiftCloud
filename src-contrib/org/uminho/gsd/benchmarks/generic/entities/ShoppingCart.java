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

import java.sql.Timestamp;
import java.util.TreeMap;

public class ShoppingCart implements Entity {

	int i_id;

	// Not used
	int SC_C_ID; // Unique identifier of the Shopping Session
	Timestamp SC_DATE;// The date and time when the CART was last updated
	float SC_SUB_TOTAL; // The gross total amount of all items in the CART
	float SC_TAX; // The tax based on the gross total amount
	float SC_SHIP_COST; // The total shipping and handling charges
	float SC_TOTAL; // The total amount of the order
	String SC_C_FNAME; // C_FNAME of the Customer
	String SC_C_LNAME; // C_LNAME of the Customer
	float SC_C_DISCOUNT; // C_DISCOUNT of the Customer

	public ShoppingCart(int i_id) {
		this.i_id = i_id;
	}

	public int getI_id() {
		return i_id;
	}

	public void setI_id(int i_id) {
		this.i_id = i_id;
	}

	public TreeMap<String, Object> getValuesToInsert() {
		TreeMap<String, Object> values = new TreeMap<String, Object>();
		return values;
	}

	public String getKeyName() {
		return "SC_ID";
	}

	@Override
	public Object copy() {
		ShoppingCart sc = new ShoppingCart(i_id);
		sc.SC_C_DISCOUNT = SC_C_DISCOUNT;
		sc.SC_C_FNAME = SC_C_FNAME;
		sc.SC_C_ID = SC_C_ID;
		sc.SC_C_LNAME = SC_C_LNAME;
		sc.SC_DATE = SC_DATE;
		sc.SC_SHIP_COST = SC_SHIP_COST;
		sc.SC_SUB_TOTAL = SC_SUB_TOTAL;
		sc.SC_TAX = SC_TAX;
		sc.SC_TOTAL = SC_TOTAL;
		return sc;
	}

}