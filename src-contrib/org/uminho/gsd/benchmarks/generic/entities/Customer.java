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
import java.sql.Timestamp;
import java.util.TreeMap;

/**
 * Costumer C_ID = C_UNAME C_PASSWD C_FNAME C_LNAME C_EMAIL C_PHONE C_ADDR_ID
 * C_DISCOUNT
 */
public class Customer implements Entity {

	int c_id;
	String C_UNAME, C_PASSWD, C_LNAME, C_FNAME;
	String C_PHONE;
	String C_EMAIL;
	Date since;
	Date lastLogin;
	Timestamp login;
	Timestamp expiration;
	double balance;
	double ytd_pmt;
	Date birthdate;
	String data;
	double C_DISCOUNT;
	int address;

	public Customer() {
	}

	public Customer(int C_ID, String C_UNAME, String C_PASSWD, String C_LNAME,
			String C_FNAME, String C_PHONE, String C_EMAIL, Date since,
			Date lastLogin, Timestamp login, Timestamp expiration,
			double balance, double ytd_pmt, Date birthdate, String data,
			double C_DISCOUNT, int address) {
		this.c_id = C_ID;
		this.C_UNAME = C_UNAME;
		this.C_PASSWD = C_PASSWD;
		this.C_LNAME = C_LNAME;
		this.C_FNAME = C_FNAME;
		this.C_PHONE = C_PHONE;
		this.C_EMAIL = C_EMAIL;
		this.since = since;
		this.lastLogin = lastLogin;
		this.login = login;
		this.expiration = expiration;
		this.balance = balance;
		this.ytd_pmt = ytd_pmt;
		this.birthdate = birthdate;
		this.data = data;
		this.C_DISCOUNT = C_DISCOUNT;
		this.address = address;
	}

	public double getC_DISCOUNT() {
		return C_DISCOUNT;
	}

	public void setC_DISCOUNT(double C_DISCOUNT) {
		this.C_DISCOUNT = C_DISCOUNT;
	}

	public String getC_EMAIL() {
		return C_EMAIL;
	}

	public void setC_EMAIL(String C_EMAIL) {
		this.C_EMAIL = C_EMAIL;
	}

	public String getC_FNAME() {
		return C_FNAME;
	}

	public void setC_FNAME(String C_FNAME) {
		this.C_FNAME = C_FNAME;
	}

	public String getC_LNAME() {
		return C_LNAME;
	}

	public void setC_LNAME(String C_LNAME) {
		this.C_LNAME = C_LNAME;
	}

	public String getC_PASSWD() {
		return C_PASSWD;
	}

	public void setC_PASSWD(String C_PASSWD) {
		this.C_PASSWD = C_PASSWD;
	}

	public String getC_PHONE() {
		return C_PHONE;
	}

	public void setC_PHONE(String c_PHONE) {
		C_PHONE = c_PHONE;
	}

	public int getAddress() {
		return address;
	}

	public void setAddress(int address) {
		this.address = address;
	}

	public double getBalance() {
		return balance;
	}

	public void setBalance(double balance) {
		this.balance = balance;
	}

	public Date getBirthdate() {
		return birthdate;
	}

	public void setBirthdate(Date birthdate) {
		this.birthdate = birthdate;
	}

	public int getC_id() {
		return c_id;
	}

	public void setC_id(int c_id) {
		this.c_id = c_id;
	}

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

	public Timestamp getExpiration() {
		return expiration;
	}

	public void setExpiration(Timestamp expiration) {
		this.expiration = expiration;
	}

	public Date getLastLogin() {
		return lastLogin;
	}

	public void setLastLogin(Date lastLogin) {
		this.lastLogin = lastLogin;
	}

	public Timestamp getLogin() {
		return login;
	}

	public void setLogin(Timestamp login) {
		this.login = login;
	}

	public Date getSince() {
		return since;
	}

	public void setSince(Date since) {
		this.since = since;
	}

	public double getYtd_pmt() {
		return ytd_pmt;
	}

	public void setYtd_pmt(double ytd_pmt) {
		this.ytd_pmt = ytd_pmt;
	}

	public String getC_UNAME() {
		return C_UNAME;
	}

	public void setC_UNAME(String C_UNAME) {
		this.C_UNAME = C_UNAME;
	}

	public TreeMap<String, Object> getValuesToInsert() {

		TreeMap<String, Object> values = new TreeMap<String, Object>();

		values.put("C_UNAME", C_UNAME);
		values.put("C_PASSWD", C_PASSWD);
		values.put("C_FNAME", C_FNAME);
		values.put("C_LNAME", C_LNAME);
		values.put("C_ADDR_ID", address);
		values.put("C_PHONE", C_PHONE);
		values.put("C_EMAIL", C_EMAIL);
		values.put("C_SINCE", since);
		values.put("C_LAST_VISIT", lastLogin);
		values.put("C_LOGIN", login);
		values.put("C_EXPIRATION", expiration);
		values.put("C_DISCOUNT", C_DISCOUNT);
		values.put("C_BALANCE", balance);
		values.put("C_YTD_PMT", ytd_pmt);
		values.put("C_BIRTHDATE", birthdate);
		values.put("C_DATA", data);

		return values;
	}

	public String getKeyName() {
		return "C_ID";
	}

	@Override
	public Object copy() {
		return new Customer(c_id, C_UNAME, C_PASSWD, C_LNAME, C_FNAME, C_PHONE,
				C_EMAIL, since, lastLogin, login, expiration, balance, ytd_pmt,
				birthdate, data, C_DISCOUNT, address);
	}

}