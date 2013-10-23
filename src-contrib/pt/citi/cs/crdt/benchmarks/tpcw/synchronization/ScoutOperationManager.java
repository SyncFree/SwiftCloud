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

import static sys.net.api.Networking.Networking;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import sys.net.api.Endpoint;
import sys.net.api.Networking.TransportProvider;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcMessage;

public class ScoutOperationManager {

    private String scoutAddress;
    private int scoutPort;
    private Endpoint scout;
    private RpcEndpoint scoutEndpoint;
    private TPCWRpc msg;
    private static BufferedOutputStream bufferedOutput;
    private OutputStream output;
    private static AtomicBoolean finished = new AtomicBoolean(false);

    public ScoutOperationManager(String scoutAddress, int portAddress, OutputStream output) {
        this.scoutAddress = scoutAddress;
        this.scoutPort = portAddress;
        this.output = output;
    }

    public void init() {
        this.scout = Networking.resolve(scoutAddress, scoutPort);
        this.scoutEndpoint = Networking.rpcConnect(TransportProvider.DEFAULT).toDefaultService();
        try {
            bufferedOutput = new BufferedOutputStream(output);
            bufferedOutput.write(("START_TIME\t" + System.currentTimeMillis() + "\n").getBytes());
            bufferedOutput
                    .write(("THREAD_ID\tOPERATION\tSTART_TIME\tOP_RECEIVED_TIME\tOP_EXECUTION_END_TIME\tTOTAL_TIME"
                            + "\n").getBytes());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void startOperation() {
        long startTime = System.currentTimeMillis();
        msg = new TPCWRpc(startTime);
        scoutEndpoint.send(scout, msg, new TPCWRpcHandler() {
            public void onReceive(TPCWRpc m) {
                final long now = System.currentTimeMillis();
                m.setTotalTime(now);
                if (m.isFinished()) {
                    terminate();
                }
                try {
                    bufferedOutput.write((m.toString() + "\n").getBytes());
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

            }

            @Override
            public void onFailure(final RpcHandle h) {
                terminate();
                System.exit(0);
            }

            @Override
            public void onReceive(final RpcMessage m) {
                terminate();
                System.exit(0);

            }
        });

    }

    public void terminate() {
        System.out.println("terminate called");
        try {

            boolean f = finished.getAndSet(true);
            if (!f) {
                bufferedOutput.write(("BENCHMARK TERMINATED").getBytes());
                bufferedOutput.write(("END_TIME\t" + System.currentTimeMillis()).getBytes());
                output.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public boolean isFinished() {
        return finished.get();
    }
}
