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
package sys.stats;

import static sys.Sys.Sys;

import java.util.HashMap;
import java.util.Map;

import sys.net.api.Endpoint;
import sys.net.impl.rpc.RpcPacket;
import sys.scheduler.PeriodicTask;
import sys.utils.XmlExternalizable;

public class RpcStats extends XmlExternalizable {
    public static double STATS_BIN_SIZE = 30.0;

    public double T0 = Sys.currentTime();

    public Map<String, BinnedTally> rpcRTT = new HashMap<String, BinnedTally>();
    public Map<String, BinnedTally> inMsgTraffic = new HashMap<String, BinnedTally>();
    public Map<String, BinnedTally> outMsgTraffic = new HashMap<String, BinnedTally>();
    public Map<String, BinnedTally> rpcExecTime = new HashMap<String, BinnedTally>();

    public RpcStats() {
        RpcStats = this;
    }

    synchronized public void logSentRpcPacket(RpcPacket pkt, Endpoint dst) {
        String type = pkt.getPayload().getClass().getName();
        valueFor(outMsgTraffic, type, true).tally(Sys.currentTime(), pkt.getSize());
    }

    synchronized public void logReceivedRpcPacket(RpcPacket pkt, Endpoint src) {
        String type = pkt.getPayload().getClass().getName();
        valueFor(inMsgTraffic, type, true).tally(Sys.currentTime(), pkt.getSize());
    }

    synchronized public void logRpcExecTime(Class<?> cl, double time) {
        valueFor(rpcExecTime, cl.getName(), true).tally(Sys.currentTime(), time);
    }

    synchronized public void logRpcRTT(Endpoint dst, double rtt) {
        valueFor(rpcRTT, dst.toString(), true).tally(Sys.currentTime(), rtt);
    }

    private synchronized <K> BinnedTally valueFor(Map<K, BinnedTally> map, K key, boolean create) {
        BinnedTally res = map.get(key);
        if (res == null && create) {
            map.put(key, res = new BinnedTally(STATS_BIN_SIZE, ""));
        }
        return res;
    }

    static double PT = 0, DL = 0, UL = 0;
    static {
        new RpcStats();

        new PeriodicTask(0, 3) {
            public void run() {
                synchronized (RpcStats) {
                    try {

                        Runtime rt = Runtime.getRuntime();
                        int idx = sys.Sys.Sys.mainClass.indexOf("Server");
                        if (idx >= 0) {
                            double elapsed = Sys.currentTime() - PT;
                            double dlrate = (Sys.downloadedBytes.get() - DL) / elapsed;
                            double ulrate = (Sys.uploadedBytes.get() - UL) / elapsed;
                            if (elapsed > 15) {
                                PT += elapsed;
                                DL = Sys.downloadedBytes.get();
                                UL = Sys.uploadedBytes.get();
                            }
                            System.err.printf("%s  [Down:  %.1f KB/s, Up: %.1f KB/s Heap: %s/%sm]\n", Sys.mainClass,
                                    dlrate / 1024, ulrate / 1024, rt.totalMemory() >> 20, rt.maxMemory() >> 20);
                        }
                        RpcStats.saveXmlTo("./tmp/" + Sys.mainClass + "-stats.xml");
                    } catch (Exception x) {
                        x.printStackTrace();
                    }
                }
            }
        };
    }

    public static RpcStats RpcStats;
}
