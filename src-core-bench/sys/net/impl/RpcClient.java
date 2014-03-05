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
package sys.net.impl;

import static sys.Sys.Sys;
import static sys.net.api.Networking.Networking;

import java.net.UnknownHostException;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import sys.net.api.Endpoint;
import sys.net.api.Networking.TransportProvider;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandle;
import sys.stats.Tally;
import sys.utils.Threading;

public class RpcClient {
    public static Logger Log = Logger.getLogger(RpcClient.class.getName());

    static double sumRTT = 0, totRTT = 0;

    public void doIt(String serverAddr) {

        RpcEndpoint endpoint = Networking.rpcConnect(TransportProvider.DEFAULT).toDefaultService();

        final Endpoint server = Networking.resolve(serverAddr, RpcServer.PORT);

        double T0 = Sys.currentTime();

        final SortedSet<Integer> values = new TreeSet<Integer>();

        final Tally rtt = new Tally("rtt");
        final Tally maxRTT = new Tally("max rtt");
        for (int n = 0;; n++) {
            synchronized (values) {
                values.add(n);
            }

            RpcHandle h = endpoint.send(server, new Request(n), new Handler() {

                @Override
                public void onFailure(RpcHandle handle) {
                    System.out.println("Send failed...");
                }

                @Override
                public void onReceive(Reply r) {
                    rtt.add(r.rtt() / 1000);
                    System.err.printf("%.1f/%.1f/%.1f - %.1f\n", rtt.min(), rtt.average(), rtt.max(), maxRTT.average());
                    if (rtt.numberObs() % 99 == 0) {
                        maxRTT.add(rtt.max());
                        rtt.init();
                    }
                    synchronized (values) {
                        values.remove(r.val);
                    }
                    sumRTT += r.rtt();
                    totRTT++;
                }

            });

            h.getReply();

            int total = n;
            if (total % 1 == 0) {
                synchronized (values) {
                    System.out.printf(endpoint.localEndpoint()
                            + " #total %d, RPCs/sec %.1f Lag %d rpcs, avg RTT %.0f us\n", total,
                            +total / (Sys.currentTime() - T0), (values.isEmpty() ? 0 : (n - values.first())), sumRTT
                                    / totRTT);
                }
            }
            // System.out.printf("\rBytes sent: %s, received: %s",
            // server.getOutgoingBytesCounter(),
            // server.getIncomingBytesCounter());
            while (values.size() > 1000)
                Threading.sleep(1);
        }
    }

    public static void main(String[] args) throws UnknownHostException {
        Log.setLevel(Level.ALL);

        String serverAddr = args.length > 0 ? args[0] : "localhost";

        sys.Sys.init();

        new RpcClient().doIt(serverAddr);
    }
}
