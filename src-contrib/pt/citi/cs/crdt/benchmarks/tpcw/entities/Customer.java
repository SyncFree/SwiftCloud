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
 *****************************************************************************/package pt.citi.cs.crdt.benchmarks.tpcw.entities;

import org.uminho.gsd.benchmarks.interfaces.Entity;
import java.util.TreeMap;

/**
 * Costumer C_ID = C_UNAME C_PASSWD C_FNAME C_LNAME C_EMAIL C_PHONE C_ADDR_ID
 * C_DISCOUNT
 */
public class Customer implements Entity {

	public String c_id;
	public String C_UNAME, C_PASSWD, C_LNAME, C_FNAME;
	public String C_PHONE;
	public String C_EMAIL;
	public String C_SINCE;
	public String C_LAST_VISIT;
	public String C_LOGIN;
	public String C_EXPIRATION;
	public double C_BALANCE;
	
	public String getC_O_LAST_ID() {
		return C_O_LAST_ID;
	}

	public void setC_O_LAST_ID(String c_O_LAST_ID) {
		C_O_LAST_ID = c_O_LAST_ID;
	}

	public double C_YTD_PMT;
	public String C_BIRTHDATE;
	public String C_DATA;
	public double C_DISCOUNT;
	public String C_ADDR_ID;
	public String C_O_LAST_ID;

	Customer() {
	}

	public Customer(String C_ID, String C_UNAME, String C_PASSWD,
			String C_LNAME, String C_FNAME, String C_PHONE, String C_EMAIL,
			String since, String lastLogin, String login, String expiration,
			double balance, double ytd_pmt, String birthdate, String data,
			double C_DISCOUNT, String address) {
		this.c_id = C_ID;
		this.C_UNAME = C_UNAME;
		this.C_PASSWD = C_PASSWD;
		this.C_LNAME = C_LNAME;
		this.C_FNAME = C_FNAME;
		this.C_PHONE = C_PHONE;
		this.C_EMAIL = C_EMAIL;
		this.C_SINCE = since;
		this.C_LAST_VISIT = lastLogin;
		this.C_LOGIN = login;
		this.C_EXPIRATION = expiration;
		this.C_BALANCE = balance;
		this.C_YTD_PMT = ytd_pmt;
		this.C_BIRTHDATE = birthdate;
		this.C_DATA = data;
		this.C_DISCOUNT = C_DISCOUNT;
		this.C_ADDR_ID = address;
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

	public String getAddress() {
		return C_ADDR_ID;
	}

	public void setAddress(String address) {
		this.C_ADDR_ID = address;
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

	public double getBalance() {
		return C_BALANCE;
	}

	public void setBalance(double balance) {
		this.C_BALANCE = balance;
	}

	public String getBirthdate() {
		return C_BIRTHDATE;
	}

	public void setBirthdate(String birthdate) {
		this.C_BIRTHDATE = birthdate;
	}

	public String getC_id() {
		return c_id;
	}

	public void setC_id(String c_id) {
		this.c_id = c_id;
	}

	public String getData() {
		return C_DATA;
	}

	public void setData(String data) {
		this.C_DATA = data;
	}

	public String getExpiration() {
		return C_EXPIRATION;
	}

	public void setExpiration(String expiration) {
		this.C_EXPIRATION = expiration;
	}

	public String getLastLogin() {
		return C_LAST_VISIT;
	}

	public void setLastLogin(String lastLogin) {
		this.C_LAST_VISIT = lastLogin;
	}

	public String getLogin() {
		return C_LOGIN;
	}

	public void setLogin(String login) {
		this.C_LOGIN = login;
	}

	public String getSince() {
		return C_SINCE;
	}

	public void setSince(String since) {
		this.C_SINCE = since;
	}

	public double getYtd_pmt() {
		return C_YTD_PMT;
	}

	public void setYtd_pmt(double ytd_pmt) {
		this.C_YTD_PMT = ytd_pmt;
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
		values.put("C_ADDR_ID", C_ADDR_ID);
		values.put("C_PHONE", C_PHONE);
		values.put("C_EMAIL", C_EMAIL);
		values.put("C_SINCE", C_SINCE);
		values.put("C_LAST_VISIT", C_LAST_VISIT);
		values.put("C_LOGIN", C_LOGIN);
		values.put("C_EXPIRATION", C_EXPIRATION);
		values.put("C_DISCOUNT", C_DISCOUNT);
		values.put("C_BALANCE", C_BALANCE);
		values.put("C_YTD_PMT", C_YTD_PMT);
		values.put("C_BIRTHDATE", C_BIRTHDATE);
		values.put("C_DATA", C_DATA);
		values.put("C_O_LAST_ID", C_O_LAST_ID);

		return values;
	}

	public String getKeyName() {
		return "C_ID";
	}

	@Override
	public Object copy() {
		return new Customer(c_id, C_UNAME, C_PASSWD, C_LNAME, C_FNAME, C_PHONE,
				C_EMAIL, C_SINCE, C_LAST_VISIT, C_LOGIN, C_EXPIRATION,
				C_BALANCE, C_YTD_PMT, C_BIRTHDATE, C_DATA, C_DISCOUNT,
				C_ADDR_ID);
	}

}