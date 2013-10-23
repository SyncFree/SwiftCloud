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

package org.uminho.gsd.benchmarks.generic.entities;

import org.uminho.gsd.benchmarks.interfaces.Entity;

import java.util.TreeMap;

public class Results implements Entity {

	int Bought;
	int TotalStock;
	String ClientID;

	public Results(int bought, int totalStock, String clientID) {
		Bought = bought;
		TotalStock = totalStock;
		ClientID = clientID;
	}

	public String getKeyName() {
		return "ITEM_ID"; // To change body of implemented methods use File |
							// Settings | File Templates.
	}

	public TreeMap<String, Object> getValuesToInsert() {
		TreeMap<String, Object> values = new TreeMap<String, Object>();

		values.put("BOUGHT", Bought);
		values.put("STOCK", TotalStock);
		values.put("CLIENT_ID", ClientID);

		return values;
	}

	@Override
	public Object copy() {
		return new Results(Bought, TotalStock, ClientID);
	}
}