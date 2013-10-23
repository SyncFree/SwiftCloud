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

import java.util.TreeMap;

/**
 * OL_ID OL_O_ID OL_I_ID OL_QTY OL_DISCOUNT OL_COMMENT
 */
public class OrderLine implements Entity {

	int OL_ID;
	int OL_O_ID;
	int OL_I_ID;
	int OL_QTY;
	double OL_DISCOUNT;
	String OL_COMMENT;

	public OrderLine(int OL_ID, int OL_O_ID, int OL_I_ID, int OL_QTY,
			double OL_DISCOUNT, String OL_COMMENT) {
		this.OL_ID = OL_ID;
		this.OL_O_ID = OL_O_ID;
		this.OL_I_ID = OL_I_ID;
		this.OL_QTY = OL_QTY;
		this.OL_DISCOUNT = OL_DISCOUNT;
		this.OL_COMMENT = OL_COMMENT;
	}

	public TreeMap<String, Object> getValuesToInsert() {

		TreeMap<String, Object> values = new TreeMap<String, Object>();

		values.put("OL_O_ID", OL_O_ID);
		values.put("OL_I_ID", OL_I_ID);
		values.put("OL_QTY", OL_QTY);
		values.put("OL_DISCOUNT", OL_DISCOUNT);
		values.put("OL_COMMENTS", OL_COMMENT);

		return values;

	}

	public String getKeyName() {
		return "OL_ID";
	}

	@Override
	public Object copy() {
		return new OrderLine(OL_ID, OL_O_ID, OL_I_ID, OL_QTY, OL_DISCOUNT,
				OL_COMMENT);
	}

}