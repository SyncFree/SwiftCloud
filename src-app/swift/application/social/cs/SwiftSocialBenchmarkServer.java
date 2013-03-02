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
package swift.application.social.cs;

import static sys.net.api.Networking.Networking;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import swift.application.social.SwiftSocial;
import swift.application.social.SwiftSocialMain;
import sys.ec2.ClosestDomain;
import sys.net.api.Networking.TransportProvider;
import sys.net.api.rpc.RpcHandle;
import sys.utils.Args;
import sys.utils.IP;

/**
 * Benchmark of SwiftSocial, based on data model derived from WaltSocial
 * prototype [Sovran et al. SOSP 2011].
 * <p>
 * Runs in parallel SwiftSocial sessions from the provided file. Sessions can be
 * distributed among different instances by specifying sessions range.
 */
public class SwiftSocialBenchmarkServer extends SwiftSocialMain {
    public static int SCOUT_PORT = 26667;

    public static void main(String[] args) {
        sys.Sys.init();

        List<String> servers = Args.subList(args, "-servers");
        dcName = ClosestDomain.closest2Domain(servers);
        System.err.println(IP.localHostAddress() + " connecting to: " + dcName);

        SwiftSocialMain.init();

        Networking.rpcBind(SCOUT_PORT, TransportProvider.DEFAULT).toService(0, new RequestHandler() {

            @Override
            public void onReceive(final RpcHandle handle, final Request m) {
                String cmdLine = m.payload;
                String sessionId = handle.remoteEndpoint().toString();

                SwiftSocial socialClient = getSession(sessionId);

                SwiftSocialMain.runCommandLine(socialClient, cmdLine);

                handle.reply(new Request("OK"));
            }
        });

        System.err.println("SwiftSocial Server Ready...");
    }

    private static void exitWithUsage() {
        System.err.println("Usage: not implemented...");
        System.exit(1);
    }

    static SwiftSocial getSession(String sessionId) {
        SwiftSocial res = sessions.get(sessionId);
        if (res == null)
            sessions.put(sessionId, res = SwiftSocialMain.getSwiftSocial());

        return res;
    }

    static Map<String, SwiftSocial> sessions = new HashMap<String, SwiftSocial>();
}
