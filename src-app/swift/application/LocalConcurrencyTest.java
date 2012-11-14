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

import java.util.Random;

import swift.client.SwiftImpl;
import swift.client.SwiftOptions;
import swift.crdt.CRDTIdentifier;
import swift.crdt.IntegerTxnLocal;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.SwiftSession;
import swift.crdt.interfaces.TxnHandle;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import sys.Sys;

/**
 * Local setup/test with one server and two clients.
 * 
 * @author annettebieniusa
 * 
 */
public class LocalConcurrencyTest {
    static String sequencerName = "localhost";

    public static void main(String[] args) {
        // start sequencer server
        DCSequencerServer.main(new String[] { "-name", sequencerName });
        // DCSequencerServer sequencer = new DCSequencerServer(sequencerName);
        // sequencer.start();

        // start DC server
        DCServer.main(new String[] { sequencerName });

        Thread[] threads = new Thread[15];
        for (int i = 0; i < 5; i++) {
            Thread client = new Thread("client" + i) {
                public void run() {
                    Sys.init();
                    SwiftSession clientServer = SwiftImpl
                            .newSingleSessionInstance(new SwiftOptions("localhost", DCConstants.SURROGATE_PORT));
                    clientCode(clientServer);
                    clientServer.stopScout(true);
                }
            };
            threads[i] = client;
            client.start();
        }
        SwiftSession checkServer = SwiftImpl.newSingleSessionInstance(new SwiftOptions("localhost", DCConstants.SURROGATE_PORT));
        boolean done = false;
        while (!done) {
            done = check(checkServer);
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        checkServer.stopScout(true);
    }

    private static void clientCode(SwiftSession server) {
        try {
            TxnHandle handle = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT,
                    false);
            IntegerTxnLocal i1 = handle.get(new CRDTIdentifier("e", "1"), false, swift.crdt.IntegerVersioned.class);
            IntegerTxnLocal i2 = handle.get(new CRDTIdentifier("e", "2"), false, swift.crdt.IntegerVersioned.class);
            Random random = new Random();
            for (int i = 0; i < 5; i++) {
                i1.add(1);
                Thread.sleep(random.nextInt(500));
                i2.add(1);
            }
            handle.commit();
            System.out.println("commit");

            System.out.println(Thread.currentThread().getName() + " finished successfully!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static boolean check(SwiftSession checkServer) {
        try {
            TxnHandle handle = checkServer.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION,
                    CachePolicy.STRICTLY_MOST_RECENT, false);
            IntegerTxnLocal i1 = handle.get(new CRDTIdentifier("e", "1"), false, swift.crdt.IntegerVersioned.class);
            IntegerTxnLocal i2 = handle.get(new CRDTIdentifier("e", "2"), false, swift.crdt.IntegerVersioned.class);
            int val1 = i1.getValue();
            int val2 = i2.getValue();

            System.out.println("Value of i1 = " + val1);
            System.out.println("Value of i2 = " + val2);
            handle.commit();
            System.out.println("commit");

            System.out.println(Thread.currentThread().getName() + " finished successfully!");
            return (val1 == 25 && val2 == 25);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
}
