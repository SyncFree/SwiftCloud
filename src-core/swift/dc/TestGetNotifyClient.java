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
package swift.dc;

import swift.client.AbstractObjectUpdatesListener;
import swift.client.SwiftImpl;
import swift.client.SwiftOptions;
import swift.crdt.CRDTIdentifier;
import swift.crdt.IntegerTxnLocal;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.SwiftSession;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;
import sys.Sys;

public class TestGetNotifyClient {
    public static void main(String[] args) {
        try {
            if (args.length != 3) {
                System.out.println("Use: jaba swift.dc.TestGetNotifyClient surrogate_node table key");
                return;
            }
            String serverNode = args[0];
            String table = args[1];
            String key = args[2];

            Sys.init();

            SwiftSession server = SwiftImpl.newSingleSessionInstance(new SwiftOptions(serverNode,
                    DCConstants.SURROGATE_PORT));
            TxnHandle handle = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT,
                    false);
            IntegerTxnLocal i1 = handle.get(new CRDTIdentifier(table, key), false, swift.crdt.IntegerVersioned.class,
                    new DummyObjectUpdatesListener());
            System.out.println("(" + table + "," + key + ") = " + i1.getValue());
            handle.commit();

            Thread.sleep(300000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(0);
    }

    static class DummyObjectUpdatesListener extends AbstractObjectUpdatesListener {
        @Override
        public void onObjectUpdate(TxnHandle txn, CRDTIdentifier id, TxnLocalCRDT<?> previousValue) {
            System.out.println("Yoohoo, the object " + id + " has changed!\n" + previousValue
                    + "\ntime notification recived = " + System.currentTimeMillis());
        }
    }
}
