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

import java.util.Random;
import java.util.TreeMap;

public class Address implements Entity {

	String street1;
	String street2;
	String city;
	String state;
	String zip;
	int country_id;
	int address_id;

	public Address(int key, String street1, String street2, String city,
			String state, String zip, int country) {

		Random r = new Random();
		this.street1 = street1;
		this.street2 = street2;
		this.city = city;
		this.state = state;
		this.zip = zip;
		this.country_id = country;
		this.address_id = key;
	}

	public int getAddress_id() {
		return address_id;
	}

	public void setAddress_id(int address_id) {
		this.address_id = address_id;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public int getCountry_id() {
		return country_id;
	}

	public void setCountry_id(int country_id) {
		this.country_id = country_id;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public String getStreet1() {
		return street1;
	}

	public void setStreet1(String street1) {
		this.street1 = street1;
	}

	public String getStreet2() {
		return street2;
	}

	public void setStreet2(String street2) {
		this.street2 = street2;
	}

	public String getZip() {
		return zip;
	}

	public void setZip(String zip) {
		this.zip = zip;
	}

	public TreeMap<String, Object> getValuesToInsert() {
		TreeMap<String, Object> values = new TreeMap<String, Object>();

		values.put("ADDR_STREET1", street1);
		values.put("ADDR_STREET2", street2);
		values.put("ADDR_CITY", city);
		values.put("ADDR_STATE", state);
		values.put("ADDR_ZIP", zip);
		values.put("ADDR_CO_ID", country_id);

		return values;
	}

	public String getKeyName() {
		return "ADDR_ID";
	}

	@Override
	public Object copy() {
		return new Address(address_id, street1, street2, city, state, zip,
				country_id);
	}

}