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
package sys.dht;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Set;
import java.util.logging.Logger;

import sys.herd.Herd;
import sys.net.api.Endpoint;

public class DHT_Node {
    private static Logger Log = Logger.getLogger("sys.dht");

    static Node self;
    static OrdinalDB db;

    protected DHT_Node() {
    }

    public static void init(String dc, String herd, Endpoint selfEndpoint, Endpoint shepard) {
        Herd.joinHerd(dc, herd, selfEndpoint, shepard);

        int delay = 10;
        System.err.printf("Waiting %s seconds for <%s, %s> membership to settle...", delay, dc, herd);
        Herd h = Herd.getHerd(dc, herd, delay);
        db = new OrdinalDB().populate(h, selfEndpoint);
        self = db.self();
        System.err.printf("Found %d node(s): %s\n", db.nodes().size(), db.nodes());
    }

    public static Set<Long> nodeKeys() {
        return db.nodeKeys();
    }

    static public boolean isHandledLocally(final String key) {
        return resolveNextHop(key).key == self.key;
    }

    static public Endpoint resolveKey(final String key) {
        return resolveNextHop(key).endpoint;
    }

    static Node resolveNextHop(String key) {
        long key2key = longHashValue(key) & Node.MAX_KEY;
        Log.finest(String.format("Hashing %s (%s) @ %s DB:%s", key, key2key, self.key, db.nodeKeys()));

        for (Node i : db.nodes(key2key))
            if (i.isOnline())
                return i;

        return self;
    }

    static public long longHashValue(String key) {
        synchronized (digest) {
            digest.reset();
            digest.update(key.getBytes());
            return new BigInteger(1, digest.digest()).longValue() >>> 1;
        }
    }

    private static MessageDigest digest;
    static {
        try {
            digest = java.security.MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }
}
