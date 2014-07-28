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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import sys.net.api.Endpoint;
import sys.net.api.Networking.TransportProvider;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandle;
import sys.utils.Threading;

public class RpcClient {
    public static Logger Log = Logger.getLogger(RpcClient.class.getName());

    static AtomicInteger g_counter = new AtomicInteger(-1);

    static AtomicLong rttSum = new AtomicLong(0);
    static AtomicLong rttCount = new AtomicLong(0);

    public void doIt(String serverAddr) {

        int index = g_counter.incrementAndGet();

        RpcEndpoint endpoint = Networking.rpcConnect(TransportProvider.INPROC).toDefaultService();

        final Endpoint server = Networking.resolve(serverAddr, RpcServer.PORT);

        double T0 = Sys.currentTime(), T = 0;

        final ConcurrentHashMap<Long, Long> values = new ConcurrentHashMap<Long, Long>();

        for (long n = 0;; n++) {

            values.put(n, System.nanoTime());
            endpoint.send(server, new Request(n), new Handler() {

                @Override
                public void onFailure(RpcHandle handle) {
                    System.out.println("Send failed...");
                }

                @Override
                public void onReceive(Reply r) {
                    long nanos = System.nanoTime() - values.remove(r.val);
                    rttSum.addAndGet(nanos / 1000);
                    rttCount.incrementAndGet();
                }

            }, 0);

            double now = Sys.currentTime();
            if (index == 0 && (now - T) > 15) {
                T = now;
                final double total = rttCount.get();
                double elapsed = Sys.currentTime() - T0;
                System.out.printf(endpoint.localEndpoint()
                        + " #total %.0f, RPCs/sec %.1f Lag %d rpcs, avg RTT %.0f us\n", total, total / elapsed,
                        values.size(), rttSum.get() / total);

                T0 = now;
                rttCount.set(0);
                rttSum.set(0);
            }

            while (values.size() > 1000)
                Threading.sleep(1);
        }
    }

    public static void main(String[] args) {
        Log.setLevel(Level.ALL);

        String serverAddr = args.length > 0 ? args[0] : "localhost";

        sys.Sys.init();

        KryoLib.register(Request.class, 0x100);
        KryoLib.register(Reply.class, 0x101);

        new RpcClient().doIt(serverAddr);
    }
}
