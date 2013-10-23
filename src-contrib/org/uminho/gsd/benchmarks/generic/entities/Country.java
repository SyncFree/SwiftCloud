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

public class Country implements Entity {

	String name;
	String currency;
	double exchange;
	int co_id;

	public Country(int co_id, String name, String currency, double exchange) {
		this.name = name;
		this.currency = currency;
		this.exchange = exchange;
		this.co_id = co_id;
	}

	public String getCo_id() {
		return name;
	}

	public String getCurrency() {
		return currency;
	}

	public void setCurrency(String currency) {
		this.currency = currency;
	}

	public double getExchange() {
		return exchange;
	}

	public void setExchange(double exchange) {
		this.exchange = exchange;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public TreeMap<String, Object> getValuesToInsert() {
		TreeMap<String, Object> values = new TreeMap<String, Object>();

		values.put("CO_NAME", name);
		values.put("CO_CURRENCY", currency);
		values.put("CO_EXCHANGE", exchange);

		return values;
	}

	public String getKeyName() {
		return "CO_ID";
	}

	@Override
	public Object copy() {
		return new Country(co_id, name, currency, exchange);
	}

}