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

public class Country implements Entity {

	public String CO_NAME;
	public String CO_CURRENCY;
	public double CO_EXCHANGE;
	public int CO_ID;

	Country() {
	}

	public Country(int co_id, String name, String currency, double exchange) {
		this.CO_NAME = name;
		this.CO_CURRENCY = currency;
		this.CO_EXCHANGE = exchange;
		this.CO_ID = co_id;
	}

	public String getCo_id() {
		return CO_NAME;
	}

	public String getCurrency() {
		return CO_CURRENCY;
	}

	public void setCurrency(String currency) {
		this.CO_CURRENCY = currency;
	}

	public double getExchange() {
		return CO_EXCHANGE;
	}

	public void setExchange(double exchange) {
		this.CO_EXCHANGE = exchange;
	}

	public String getName() {
		return CO_NAME;
	}

	public void setName(String name) {
		this.CO_NAME = name;
	}

	public TreeMap<String, Object> getValuesToInsert() {
		TreeMap<String, Object> values = new TreeMap<String, Object>();

		values.put("CO_NAME", CO_NAME);
		values.put("CO_CURRENCY", CO_CURRENCY);
		values.put("CO_EXCHANGE", CO_EXCHANGE);
		values.put("CO_ID", CO_ID);

		return values;
	}

	public String getKeyName() {
		return "CO_ID";
	}

	@Override
	public Object copy() {
		return new Country(CO_ID, CO_NAME, CO_CURRENCY, CO_EXCHANGE);
	}

}