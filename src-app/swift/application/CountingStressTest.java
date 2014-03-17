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
package swift.application;

import swift.client.SwiftImpl;
import swift.client.SwiftOptions;
import swift.crdt.IntegerCRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.CachePolicy;
import swift.crdt.core.IsolationLevel;
import swift.crdt.core.SwiftSession;
import swift.crdt.core.TxnHandle;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;
import sys.Sys;

/**
 * Manual test client that stresses the system correctness by issuing mixed
 * workload of transactions with different isolation levels.
 * 
 * @author mzawirski
 */
public class CountingStressTest {
    private static final int TRANSACTIONS_PER_CLIENT = 10;
    private static final int CLIENTS_NUMBER = 5;
    static String sequencerName = "localhost";

    public static void main(String[] args) throws NetworkException, WrongTypeException, NoSuchObjectException,
            VersionNotFoundException {
        // start sequencer server
        DCSequencerServer.main(new String[] { "-name", sequencerName });
        // DCSequencerServer sequencer = new DCSequencerServer(sequencerName);
        // sequencer.start();

        // start DC server
        DCServer.main(new String[] { sequencerName });

        Sys.init();
        final Thread[] clientThreads = new Thread[CLIENTS_NUMBER];
        for (int i = 0; i < clientThreads.length; i++) {
            final int id = i;
            Thread clientThread = new Thread("client" + i) {
                public void run() {
                    SwiftSession client = SwiftImpl.newSingleSessionInstance(new SwiftOptions("localhost",
                            DCConstants.SURROGATE_PORT));
                    runTransactions(client, id);
                    client.stopScout(true);
                }
            };
            clientThreads[i] = clientThread;
            clientThread.start();
        }

        for (int i = 0; i < clientThreads.length; i++) {
            try {
                clientThreads[i].join();
            } catch (InterruptedException e) {
            }
        }

        // Check result
        SwiftSession client = SwiftImpl.newSingleSessionInstance(new SwiftOptions("localhost",
                DCConstants.SURROGATE_PORT));
        TxnHandle txn = client.beginTxn(level, CachePolicy.STRICTLY_MOST_RECENT, false);
        IntegerCRDT i1 = txn.get(new CRDTIdentifier("tests", "1"), true, swift.crdt.IntegerCRDT.class,
                TxnHandle.UPDATES_SUBSCRIBER);
        assert (i1.getValue() == CLIENTS_NUMBER * TRANSACTIONS_PER_CLIENT);
        txn.commit();

        System.out.println("clients done with processing, stopping gracefully");
        System.exit(0);
    }

    static IsolationLevel level = IsolationLevel.SNAPSHOT_ISOLATION;

    private static void runTransactions(SwiftSession client, final int clientId) {
        try {
            for (int i = 0; i < TRANSACTIONS_PER_CLIENT; i++) {
                TxnHandle txn = client.beginTxn(level, CachePolicy.STRICTLY_MOST_RECENT, false);
                IntegerCRDT i1 = txn.get(new CRDTIdentifier("tests", "1"), true, swift.crdt.IntegerCRDT.class,
                        TxnHandle.UPDATES_SUBSCRIBER);
                i1.add(1);

                txn.commitAsync(null);
            }
            System.out.printf("client%d done\n", clientId);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
