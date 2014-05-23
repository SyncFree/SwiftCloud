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

import static java.lang.System.exit;
import static sys.net.api.Networking.Networking;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import swift.application.social.Commands;
import swift.application.social.SwiftSocialBenchmark;
import swift.application.social.SwiftSocialOps;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcEndpoint;
import sys.utils.Args;

/**
 * Benchmark of SwiftSocial, based on data model derived from WaltSocial
 * prototype [Sovran et al. SOSP 2011].
 * <p>
 * Runs in parallel SwiftSocial sessions from the provided file. Sessions can be
 * distributed among different instances by specifying sessions range.
 */
public class SwiftSocialBenchmarkClient extends SwiftSocialBenchmark {

    Endpoint server;


    public void init(String[] args) {
        int port = SwiftSocialBenchmarkServer.SCOUT_PORT + Args.valueOf(args, "-instance", 0);
        server = Networking.resolve(Args.valueOf(args, "-servers", "localhost"), port);
    }

    @Override
    public Commands runCommandLine(SwiftSocialOps socialClient, String cmdLine) {
        String[] toks = cmdLine.split(";");
        final Commands cmd = Commands.valueOf(toks[0].toUpperCase());
        switch (cmd) {
        case LOGIN:
        case LOGOUT:
        case READ:
        case SEE_FRIENDS:
        case FRIEND:
        case STATUS:
        case POST:
            Reply reply = endpointFor(socialClient).request(server, new Request(cmdLine));

            break;
        default:
            System.err.println("Can't parse command line :" + cmdLine);
            System.err.println("Exiting...");
            System.exit(1);
        }
        return cmd;
    }

    Map<SwiftSocialOps, RpcEndpoint> endpoints = new ConcurrentHashMap<SwiftSocialOps, RpcEndpoint>();

    RpcEndpoint endpointFor(SwiftSocialOps session) {
        RpcEndpoint res = endpoints.get(session);
        if (res == null)
            endpoints.put(session, res = Networking.rpcConnect().toDefaultService());
        return res;
    }

    public static void main(String[] args) {
        sys.Sys.init();

        SwiftSocialBenchmarkClient client = new SwiftSocialBenchmarkClient();
        if (args.length == 0) {

            DCSequencerServer.main(new String[] { "-name", "X0" });

            args = new String[] { "-servers", "localhost", "-threads", "1" };

            DCServer.main(args);
            SwiftSocialBenchmarkServer.main(args);

            client.init(args);
            client.initDB(args);
            client.doBenchmark(args);
            exit(0);
        }

        if (args[0].equals("-init")) {
            client.init(args);
            client.initDB(args);
            exit(0);
        }

       if (args[0].equals("-run")) {
            client.init(args);
            client.doBenchmark(args);
            exit(0);
        }
    }
}
