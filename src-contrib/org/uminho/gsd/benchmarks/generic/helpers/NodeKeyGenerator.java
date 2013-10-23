/*
 * *********************************************************************
 * Copyright (c) 2010 Pedro Gomes and Universidade do Minho.
 * All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * ********************************************************************
 */

package org.uminho.gsd.benchmarks.generic.helpers;

import java.util.UUID;

import org.uminho.gsd.benchmarks.interfaces.KeyGenerator;

public class NodeKeyGenerator implements KeyGenerator {

    int nodeID;

    public NodeKeyGenerator(int nodeID) {
        this.nodeID = nodeID;
    }

    public synchronized Object getNextKey() {

        // long timestamp = Long.MAX_VALUE - System.currentTimeMillis();
        // //max long as 19 characters
        // String key = timestamp + "";
        // int length = key.length();
        //
        // String prefix = "";
        // for (int i = length; i < 19; i++) {
        // prefix = "0" + prefix;
        // }
        //
        // key = key + "." + nodeID;

        return UUID.randomUUID().toString() + "." + nodeID;
    }

    public synchronized Object getNextKey(int client) {

        // long timestamp = Long.MAX_VALUE - System.currentTimeMillis();
        // //max long as 19 characters
        // String key = timestamp + "";
        // int length = key.length();
        //
        // String prefix = "";
        // for (int i = length; i < 19; i++) {
        // prefix = "0" + prefix;
        // }
        //
        // key = key + "." + nodeID+ "."+client;

        return UUID.randomUUID().toString() + "." + nodeID + "." + client;
    }
}