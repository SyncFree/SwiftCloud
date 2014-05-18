/*****************************************************************************
 * Copyright 2011-2014 INRIA
 * Copyright 2011-2014 Universidade Nova de Lisboa
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

import sys.net.api.Endpoint;
import sys.net.api.Networking;

/**
 * 
 * @author smd
 * 
 */
public class Node {

    static final int NODE_KEY_LENGTH = 10;
    public static final long MAX_KEY = (1L << NODE_KEY_LENGTH) - 1L;

    public long key;
    public Endpoint endpoint;
    public Endpoint dhtEndpoint;
    public String datacenter;

    public Node() {
    }

    protected Node(Node other) {
        endpoint = other.endpoint;
        dhtEndpoint = other.dhtEndpoint;
        datacenter = other.datacenter;
        key = locator2key(endpoint.locator());
    }

    public Node(Endpoint endpoint) {
        this(endpoint, "?");
    }

    public Node(Endpoint endpoint, String datacenter) {
        this.endpoint = endpoint;
        this.datacenter = datacenter;
        this.key = locator2key(endpoint.locator());
        this.dhtEndpoint = Networking.Networking.resolve(endpoint.getHost(), DHT_Node.DHT_PORT);
    }

    public Node(long key, Endpoint endpoint) {
        this.key = key;
        this.endpoint = endpoint;
        this.dhtEndpoint = Networking.Networking.resolve(endpoint.getHost(), DHT_Node.DHT_PORT);
    }

    @Override
    public int hashCode() {
        return (int) ((key >>> 32) ^ key);
    }

    public boolean equals(Object other) {
        return other != null && ((Node) other).key == key;
    }

    public String getDatacenter() {
        return datacenter;
    }

    public boolean isOnline() {
        return true;
    }

    public boolean isOffline() {
        return !isOnline();
    }

    @Override
    public String toString() {
        return "" + key + " -> " + endpoint;
    }

    private static long locator2key(Object locator) {
        return (DHT_Node.longHashValue(locator.toString()) >>> 1) & MAX_KEY;
    }
}