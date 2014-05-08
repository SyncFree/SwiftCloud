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
package swift.application.adservice;

import swift.client.SwiftImpl;
import swift.client.SwiftOptions;
import swift.crdt.core.CachePolicy;
import swift.crdt.core.IsolationLevel;
import swift.crdt.core.SwiftSession;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import sys.Sys;

public class SwiftAdvertisementTest {
    private static String sequencerName = "localhost";

    public static void main(String[] args) {
        DCSequencerServer.main(new String[] { "-name", sequencerName });
        DCServer.main(new String[] { sequencerName });

        Sys.init();
        SwiftSession clientServer = SwiftImpl.newSingleSessionInstance(new SwiftOptions("localhost",
                DCConstants.SURROGATE_PORT));
        SwiftAdservice client = new SwiftAdservice(clientServer, IsolationLevel.SNAPSHOT_ISOLATION,
                CachePolicy.STRICTLY_MOST_RECENT, false, false, "localhost");

        // Marek: commented it out to avoid compilation error.
        // client.addAd("Ad", 100);
        client.viewAd("Ad");

        clientServer.stopScout(true);
        System.exit(0);
    }
}
