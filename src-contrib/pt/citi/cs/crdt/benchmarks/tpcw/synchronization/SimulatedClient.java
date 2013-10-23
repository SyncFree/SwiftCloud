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
package pt.citi.cs.crdt.benchmarks.tpcw.synchronization;

import java.io.FileOutputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import sys.Sys;

public class SimulatedClient {

    private static void doIt(String hostname, int port, String outputFileName) {
        ScoutOperationManager proto = null;
        try {
            proto = new ScoutOperationManager(hostname, port, new FileOutputStream(outputFileName));
            proto.init();
            while (!proto.isFinished()) {
                proto.startOperation();
            }
            proto.terminate();
        } catch (Exception e) {
            if (proto != null)
                proto.terminate();
            System.exit(0);
        }

    }

    // Default Port 8777

    public static void main(String[] args) {
        Sys.init();
        final String hostname = args[0];
        final int port = Integer.parseInt(args[1]);
        final String outputFileName = args[2];
        // final String zooAddress = args[4];
        // final int numCoord = Integer.parseInt(args[5]);

        // Barrier syncBarrier = new SyncPrimitive.Barrier(zooAddress, "/"
        // + "TPCW2", numCoord * 2);
        // try {
        // syncBarrier.enter();
        // System.out.println("client joinned the barrier");
        // Thread.sleep(5000);
        // syncBarrier.leave();
        // } catch (KeeperException e) {
        // e.printStackTrace();
        // } catch (InterruptedException e) {
        // e.printStackTrace();
        // }
        // System.out.println("All scouts finished pre-fetch phase");
        
        ExecutorService executor = Executors.newFixedThreadPool(4);
        for (int i = 0; i < Integer.parseInt(args[3]); i++) {
            executor.execute( new Runnable() {
                @Override
                public void run() {
                    doIt(hostname, port, outputFileName);
                }
            });
        }

    }
}
