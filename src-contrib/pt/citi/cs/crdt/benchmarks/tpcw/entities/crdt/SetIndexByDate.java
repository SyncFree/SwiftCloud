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

import swift.clocks.CausalityClock;
import swift.crdt.SetVersioned;
import swift.crdt.interfaces.CRDTUpdate;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;

public class SetIndexByDate extends SetVersioned<OrderInfo, SetIndexByDate> {

    private static final long serialVersionUID = 1L;

    public SetIndexByDate() {
    }

    @Override
    protected TxnLocalCRDT<SetIndexByDate> getTxnLocalCopyImpl(CausalityClock versionClock, TxnHandle txn) {
        final SetIndexByDate creationState = isRegisteredInStore() ? null : new SetIndexByDate();
        SetTxnLocalIndexByDate localView = new SetTxnLocalIndexByDate(id, txn, versionClock, creationState,
                getValue(versionClock));
        return localView;
    }

    @Override
    protected void execute(CRDTUpdate<SetIndexByDate> op) {
        op.applyTo(this);
    }

    @Override
    public SetIndexByDate copy() {
        SetIndexByDate copy = new SetIndexByDate();
        copyLoad(copy);
        copyBase(copy);
        return copy;
    }
}
