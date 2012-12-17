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

import static sys.net.api.Networking.Networking;

import java.util.ArrayList;
import java.util.List;

import swift.application.swiftdoc.SwiftDocOps;
import swift.application.swiftdoc.SwiftDocPatchReplay;
import swift.application.swiftdoc.TextLine;
import swift.application.swiftdoc.cs.msgs.AckHandler;
import swift.application.swiftdoc.cs.msgs.AppRpcHandler;
import swift.application.swiftdoc.cs.msgs.BeginTransaction;
import swift.application.swiftdoc.cs.msgs.CommitTransaction;
import swift.application.swiftdoc.cs.msgs.InitScoutServer;
import swift.application.swiftdoc.cs.msgs.InsertAtom;
import swift.application.swiftdoc.cs.msgs.RemoveAtom;
import swift.application.swiftdoc.cs.msgs.ServerReply;
import sys.net.api.Endpoint;
import sys.net.api.Networking.TransportProvider;
import sys.net.api.rpc.RpcEndpoint;
import sys.utils.Threading;

/**
 * 
 * @author smduarte
 * 
 */
public class SwiftDocClient {

    public static void main(String[] args) {
        System.out.println("SwiftDoc Client start!");

        if (args.length == 0)
            args = new String[] { "localhost", "1" };

        sys.Sys.init();

        final String server = args[0];

        Threading.newThread("client1", true, new Runnable() {
            public void run() {
                runClient1Code(server);
            }
        }).start();

        Threading.newThread("client2", true, new Runnable() {
            public void run() {
                runClient2Code(server);
            }
        }).start();
    }

    static void runClient1Code(String server) {
        Endpoint srv = Networking.resolve(server, SwiftDocServer.PORT1);
        client1Code(srv);
    }

    static void runClient2Code(String server) {
        Endpoint srv = Networking.resolve(server, SwiftDocServer.PORT2);
        client2Code(srv);
    }

    static final int timeout = SwiftDocServer.synchronousOps ? 5000 : 0;

    /*
     * Replay the document patching operations. Each patch will be cause an rpc
     * call for each update operation performed on the CRDT. -beginTransaction
     * -insertAt -removeAt -commitTransaction
     * 
     * read operations are performed locally on a mirror version of the
     * document...
     */
    static void client1Code(final Endpoint server) {
        final int TIMEOUT = Integer.MAX_VALUE >> 1;

        final RpcEndpoint endpoint = Networking.rpcConnect(TransportProvider.DEFAULT).toDefaultService();

        final List<Long> results = new ArrayList<Long>();

        SwiftDocPatchReplay<TextLine> player = new SwiftDocPatchReplay<TextLine>();

        endpoint.send(server, new InitScoutServer(), new AppRpcHandler() {
            public void onReceive(final ServerReply r) {
                synchronized (results) {
                    for (TextLine i : r.atoms) {
                        System.out.println(i.latency());
                    }
                }
                System.err.println("Got: " + r.atoms.size() + "/" + results.size());
            }

        });

        final AckHandler ackHandler = new AckHandler();

        try {
            player.parseFiles(new SwiftDocOps<TextLine>() {
                List<TextLine> mirror = new ArrayList<TextLine>();

                @Override
                public void begin() {
                    endpoint.send(server, new BeginTransaction(), ackHandler, timeout);
                }

                @Override
                public void add(int pos, TextLine atom) {
                    endpoint.send(server, new InsertAtom(atom, pos), ackHandler, timeout);
                    mirror.add(pos, atom);
                }

                public TextLine remove(int pos) {
                    endpoint.send(server, new RemoveAtom(pos), ackHandler, timeout);
                    return mirror.remove(pos);
                }

                @Override
                public void commit() {
                    endpoint.send(server, new CommitTransaction(), ackHandler);
                    Threading.sleep(1000);
                }

                @Override
                public TextLine get(int pos) {
                    return mirror.get(pos);
                }

                @Override
                public int size() {
                    return mirror.size();
                }

                @Override
                public TextLine gen(String s) {
                    return new TextLine(s);
                }
            });
        } catch (Exception x) {
            x.printStackTrace();
        }

        double t, t0 = System.currentTimeMillis() + 30000;
        while ((t = System.currentTimeMillis()) < t0) {
            System.err.printf("\rWaiting: %s", (t0 - t) / 1000);
            Threading.sleep(10);
        }

        synchronized (results) {
            for (Long i : results)
                System.out.printf("%s\n", i);
        }
        System.exit(0);
    }

    // Echo the atoms received to the server...
    static void client2Code(final Endpoint server) {
        final Object barrier = new Object();

        final AckHandler ackHandler = new AckHandler();

        final RpcEndpoint endpoint = Networking.rpcConnect(TransportProvider.DEFAULT).toDefaultService();

        try {
            endpoint.send(server, new InitScoutServer(), new AppRpcHandler() {
                public void onReceive(final ServerReply r) {

                    Threading.newThread(true, new Runnable() {
                        public void run() {
                            synchronized (barrier) {
                                endpoint.send(server, new BeginTransaction(), ackHandler, timeout);
                                for (TextLine i : r.atoms)
                                    endpoint.send(server, new InsertAtom(i, -1), ackHandler, timeout);

                                endpoint.send(server, new CommitTransaction(), ackHandler);
                            }
                        }
                    }).start();
                }
            });

        } catch (Exception x) {
            x.printStackTrace();
        }
    }
}
