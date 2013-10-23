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

import pt.citi.cs.crdt.benchmarks.tpcw.entities.BestSellerEntry;
import swift.clocks.CausalityClock;
import swift.crdt.SetVersioned;
import swift.crdt.interfaces.CRDTUpdate;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;

public class SetBestSellers extends SetVersioned<BestSellerEntry, SetBestSellers> {

    private static final long serialVersionUID = 1L;

    public SetBestSellers() {
    }

    @Override
    protected TxnLocalCRDT<SetBestSellers> getTxnLocalCopyImpl(CausalityClock versionClock, TxnHandle txn) {
        final SetBestSellers creationState = isRegisteredInStore() ? null : new SetBestSellers();
        SetTxnLocalBestSellers localView = new SetTxnLocalBestSellers(id, txn, versionClock, creationState, getValue(versionClock));
        return localView;
    }

    @Override
    protected void execute(CRDTUpdate<SetBestSellers> op) {
        op.applyTo(this);
    }

    @Override
    public SetBestSellers copy() {
    	SetBestSellers copy = new SetBestSellers();
        copyLoad(copy);
        copyBase(copy);
        return copy;
    }
}
