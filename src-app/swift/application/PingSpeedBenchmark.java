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
package swift.application;

import java.util.logging.Logger;

import swift.client.SwiftImpl;
import swift.client.SwiftOptions;
import swift.crdt.core.CachePolicy;
import swift.crdt.core.IsolationLevel;
import swift.crdt.core.SwiftSession;
import swift.dc.DCConstants;
import sys.Sys;

/**
 * Local setup/test with one server and two clients.
 * 
 * @author annettebieniusa
 * 
 */
public class PingSpeedBenchmark {
    private static String dcName;
    private static int dcPort;
    private static int clientId;
    private static Logger logger = Logger.getLogger("swift.application");

    public static void main(String[] args) {
        if (args.length != 6) {
            System.out
                    .println("Usage: [surrogate address] [number of iterations] [client id (1|2)] [isolationLevel] [cachePolicy] [notifications (true|false)]");
            return;
        } else {
            dcName = args[0];
            int pos = dcName.indexOf(":");
            if (pos != -1) {
                dcPort = Integer.parseInt(dcName.substring(pos + 1));
                dcName = dcName.substring(0, pos);
            } else
                dcPort = DCConstants.SURROGATE_PORT;
            PingSpeedTest.iterations = Integer.parseInt(args[1]);
            clientId = Integer.parseInt(args[2]);
            PingSpeedTest.isolationLevel = IsolationLevel.valueOf(args[3]);
            PingSpeedTest.cachePolicy = CachePolicy.valueOf(args[4]);
            PingSpeedTest.notifications = Boolean.parseBoolean(args[5]);
        }

        System.out.print("PingSpeedBenchmark ");
        for (String s : args) {
            System.out.print(s + " ");
        }
        System.out.println("");

        logger.info("Initializing the system");
        Sys.init();
        SwiftSession swift = SwiftImpl.newSingleSessionInstance(new SwiftOptions(dcName, dcPort));

        if (clientId == 1) {
            logger.info("Starting client 1");
            PingSpeedTest.runClient1(swift);
        } else if (clientId == 2) {
            logger.info("Starting client 2");
            PingSpeedTest.runClient2(swift);
        }
    }
}
