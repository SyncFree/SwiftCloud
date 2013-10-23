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

import java.util.Date;

import swift.crdt.interfaces.Copyable;

public class AuthorIndex implements Copyable {

	public String A_FNAME;
	public String A_LNAME;
	public String I_TITLE;
	public String I_ID;
	public long DATE;
	
	
	AuthorIndex() {
	}

	public AuthorIndex(String a_FNAME, String a_LNAME, String i_TITLE, long date, String item_id) {
		super();
		A_FNAME = a_FNAME;
		A_LNAME = a_LNAME;
		I_TITLE = i_TITLE;
		DATE = date;
		I_ID = item_id;
	}

	public AuthorIndex(String a_FNAME, String a_LNAME, String i_TITLE,
			long date) {
		super();
		A_FNAME = a_FNAME;
		A_LNAME = a_LNAME;
		I_TITLE = i_TITLE;
		DATE = date;
	}

	public String getA_FNAME() {
		return A_FNAME;
	}

	public void setA_FNAME(String a_FNAME) {
		A_FNAME = a_FNAME;
	}

	public String getA_LNAME() {
		return A_LNAME;
	}

	public void setA_LNAME(String a_LNAME) {
		A_LNAME = a_LNAME;
	}

	public String getI_TITLE() {
		return I_TITLE;
	}

	public void setI_TITLE(String i_TITLE) {
		I_TITLE = i_TITLE;
	}

	@Override
	public Object copy() {
		return new AuthorIndex(A_FNAME, A_LNAME, I_TITLE, DATE);
	}

	public String getI_ID() {
		return I_ID;
	}

	public Date getDate() {
		return new Date(DATE);
	}

}
