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
package swift.application.swiftdoc.cs;

import java.util.Arrays;
import java.util.logging.Logger;

import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import sys.Sys;

/**
 * 
 * 
 * @author smduarte
 * 
 */
public class SwiftDocBenchmarkClient {

    private static int clientId;
    private static String scoutName, dcName;
    private static Logger logger = Logger.getLogger("swift.application.swiftdoc.client");

    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.out.println("-->" + Arrays.asList(args));
            System.out.println("Usage: [scout address] [client id (1|2)] [DC address]");
            return;
        } else {
            
            scoutName = args[0];
            clientId = Integer.parseInt(args[1]);
            dcName = args[2];
            logger.info("Initializing the system");

            Sys.init();
            DCSequencerServer.main(new String[]{"-name", "localhost"});

            // start DC server
            DCServer.main(new String[]{"localhost"});

            if (clientId == 1) {
                logger.info("Starting client 1");
                SwiftDocClient2.runClient1Code( scoutName, dcName );
            } else if (clientId == 2) {
                logger.info("Starting client 2");
                SwiftDocClient2.runClient2Code( scoutName );
            }
        }
    }
}
