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
import sys.stats.common.BinnedTally;

public class RpcStats {
    private static double STATS_BIN_SIZE = 60.0;

    double T0 = Sys.currentTime();

    Map<Endpoint, BinnedTally> rpcRTT = new HashMap<Endpoint, BinnedTally>();
    Map<String, BinnedTally> inMsgTraffic = new HashMap<String, BinnedTally>();
    Map<String, BinnedTally> outMsgTraffic = new HashMap<String, BinnedTally>();
    Map<Class<?>, BinnedTally> rpcExecTime = new HashMap<Class<?>, BinnedTally>();

    protected RpcStats() {
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
        valueFor(rpcExecTime, cl, true).tally(Sys.currentTime(), time);
    }

    synchronized public void logRpcRTT(Endpoint dst, double rtt) {
        valueFor(rpcRTT, dst, true).tally(Sys.currentTime(), rtt);
    }

    private synchronized <K> BinnedTally valueFor(Map<K, BinnedTally> map, K key, boolean create) {
        BinnedTally res = map.get(key);
        if (res == null && create) {
            map.put(key, res = new BinnedTally(STATS_BIN_SIZE, ""));
        }
        return res;
    }

    static {
        RpcStats = new RpcStats();

        // new PeriodicTask(0, 30) {
        // public void run() {
        // synchronized (RpcStats) {
        // try {
        // //System.err.printf( "%s %.1fKB/s\n", Sys.mainClass,
        // (Sys.downloadedBytes.get()/1024) / Sys.currentTime() );
        // //RpcStats.saveXmlTo("./tmp/" + Sys.mainClass + "-stats.xml");
        // } catch (Exception x) {
        // x.printStackTrace();
        // }
        // }
        // }
        // };
    }

    public static RpcStats RpcStats;
}
