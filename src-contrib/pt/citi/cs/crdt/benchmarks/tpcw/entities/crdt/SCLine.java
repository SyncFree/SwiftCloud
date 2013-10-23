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
package pt.citi.cs.crdt.benchmarks.tpcw.entities.crdt;

import pt.citi.cs.crdt.benchmarks.tpcw.entities.TPCWNamingScheme;
import swift.crdt.CRDTIdentifier;
import swift.crdt.IntegerTxnLocal;
import swift.crdt.IntegerVersioned;
import swift.crdt.interfaces.Copyable;
import swift.crdt.interfaces.TxnHandle;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

public class SCLine implements Copyable {

	private CRDTIdentifier SCL_QTY;

	private int I_ID;
	private String SC_ID;
	private double SCL_COST;
	private double SCL_SRP;
	private String SCL_TITLE;
	private String SCL_BACKING;

	SCLine() {
	}

	public SCLine(int itemID, String shoppingCartID) {
		this.I_ID = itemID;
		this.SC_ID = shoppingCartID;
		this.SCL_QTY = TPCWNamingScheme.forSCLine(itemID+"", shoppingCartID);
	}

	public int getI_ID() {
		return I_ID;
	}

	public double getSCL_COST() {
		return SCL_COST;
	}

	public double getSCL_SRP() {
		return SCL_SRP;
	}

	public String getSCL_TITLE() {
		return SCL_TITLE;
	}

	public String getSCL_BACKING() {
		return SCL_BACKING;
	}

	public int getItemId() {
		return I_ID;
	}

	public int getSCL_QTY(TxnHandle txh) throws WrongTypeException,
			NoSuchObjectException, VersionNotFoundException, NetworkException {
		IntegerTxnLocal quantity = txh.get(SCL_QTY, true,
				IntegerVersioned.class);
		return quantity.getValue();
	}

	public void addSCL_QTY(int qTY, TxnHandle txh) throws WrongTypeException,
			NoSuchObjectException, VersionNotFoundException, NetworkException {
		IntegerTxnLocal quantity = txh.get(SCL_QTY, true,
				IntegerVersioned.class);
		quantity.add(qTY);
	}

	@Override
	public Object copy() {
		return new SCLine(this.I_ID, this.SC_ID);
	}

	public void setSCL_COST(double sCL_COST) {
		SCL_COST = sCL_COST;
	}

	public void setSCL_SRP(double sCL_SRP) {
		SCL_SRP = sCL_SRP;
	}

	public void setSCL_BACKING(String sCL_BACKING) {
		SCL_BACKING = sCL_BACKING;
	}

	public void setSCL_TITLE(String sCL_TITLE) {
		SCL_TITLE = sCL_TITLE;
	}

	public void setI_ID(int SCL_ID) {
		this.I_ID = SCL_ID;

	}

}
