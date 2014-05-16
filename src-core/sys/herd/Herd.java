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
package sys.herd;

import static sys.net.api.Networking.Networking;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import sys.herd.proto.HerdProtoHandler;
import sys.herd.proto.JoinHerdReply;
import sys.herd.proto.JoinHerdRequest;
import sys.net.api.Endpoint;
import sys.net.api.Networking.TransportProvider;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandle;
import sys.scheduler.PeriodicTask;
import sys.utils.IP;
import sys.utils.Threading;

/**
 * 
 * Simple manager for coordinating execution of multiple swiftsocial instances
 * in a single deployment...
 * 
 * @author smduarte
 * 
 */
public class Herd extends HerdProtoHandler {
    private static Logger Log = Logger.getLogger(Herd.class.getName());

    private static boolean inited = false;
    private static final int PORT = 29777;

    String dc;
    String herd;
    Set<Endpoint> sheep;

    static Endpoint shepard;
    static long lastChange = System.currentTimeMillis();
    volatile static Map<String, Map<String, Herd>> herds = new HashMap<String, Map<String, Herd>>();
    volatile static boolean stopProbing = false;

    Herd() {
    }

    Herd(String dc, String herd) {
        this.dc = dc;
        this.herd = herd;
        this.sheep = new HashSet<Endpoint>();
    }

    public String toString() {
        return sheep.toString();
    }

    public String dc() {
        return dc;
    }

    public String herd() {
        return herd;
    }

    public Set<Endpoint> sheep() {
        return sheep;
    }

    static long age() {
        return (System.currentTimeMillis() - lastChange);
    }

    public static void setDefaultShepard(String shepardAddress) {
        shepard = Networking.resolve(shepardAddress, PORT);
    }

    public static void joinHerd(final String dc, final String herd, final Endpoint endpoint) {
        joinHerd(dc, herd, endpoint, shepard);
    }

    public static void joinHerd(final String dc, final String herd, final Endpoint endpoint, Endpoint shepardAddress) {

        final Endpoint shepard = Networking.resolve(shepardAddress.getHost(), PORT);
        final RpcEndpoint sock = Networking.rpcConnect(TransportProvider.DEFAULT).toDefaultService();

        Log.info(IP.localHostname() + " Contacting shepard at: " + shepard + " to join: " + herd);
        System.err.println(IP.localHostname() + " Contacting shepard at: " + shepard + " to join: " + herd);

        new PeriodicTask(2.0, 1.0) {
            public void run() {
                try {
                    sock.send(shepard, new JoinHerdRequest(dc, herd, endpoint), new HerdProtoHandler() {
                        public void onReceive(JoinHerdReply r) {
                            herds = r.herds();
                            lastChange = System.currentTimeMillis() - r.age();
                            Log.info(String.format("Received Herd information: lastChange : [%.1f s] ago, data: %s\n",
                                    age() / 1000.0, herds));

                            Threading.synchronizedNotifyAllOn(herds);
                        }
                    }, 0);
                    if (stopProbing)
                        cancel();

                } catch (Exception x) {
                    Log.warning("Cannot connect to shepard at: " + shepard + " to join: " + herd);
                }
            }
        };
    }

    synchronized static public void initServer() {
        if (!inited)
            try {
                Networking.rpcBind(PORT, TransportProvider.DEFAULT).toService(0, new HerdProtoHandler() {
                    public void onReceive(RpcHandle conn, JoinHerdRequest r) {

                        JoinHerdReply reply;
                        synchronized (herds) {
                            if (getHerd(r.dc(), r.herd()).sheep.add(r.sheep())) {
                                lastChange = System.currentTimeMillis();
                            }
                            reply = new JoinHerdReply(age(), herds);
                        }
                        conn.reply(reply);
                    }
                });
                Log.info(IP.localHostname() + " Started Herd server @ " + IP.localHostAddressString());
                inited = true;
            } catch (Exception x) {
                // Log.warning(IP.localHostname() +
                // " Herd is already running???");
            }
    }

    static Map<String, Herd> getHerds(String dc) {
        synchronized (herds) {
            Map<String, Herd> res = herds.get(dc);
            if (res == null)
                herds.put(dc, res = new HashMap<String, Herd>());
            return res;
        }
    }

    static Herd getHerd(String dc, String herd) {
        synchronized (herds) {
            Herd h = getHerds(dc).get(herd);
            if (h == null)
                getHerds(dc).put(herd, h = new Herd(dc, herd));
            return h;
        }
    }

    public static Herd getHerd(String dc, String herd, int minimumAge, boolean stop) {
        synchronized (herds) {
            while (age() / 1000 < minimumAge) {
                Threading.synchronizedWaitOn(herds, 100);
            }
            stopProbing = stop;
            return getHerd(dc, herd);
        }
    }

    static {
        initServer();
    }
}
