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
package swift.dc;

import static sys.net.api.Networking.Networking;
import sys.Sys;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandle;
import sys.utils.Threading;

public class RpcClient {

    public static void main(String[] args) {

        Sys.init();

        RpcEndpoint endpoint = Networking.rpcConnect().toDefaultService();
        final Endpoint server = Networking.resolve("localhost", DCConstants.SURROGATE_PORT);

        for (;;) {
            endpoint.send(server, new Request(), new Handler() {

                public void onFailure(RpcHandle handler) {
                    System.out.println("Client Send failed...");
                }

                public void onReceive(RpcHandle conn, Reply r) {
                    System.out.println("Client Got: " + r + " from:" + conn.remoteEndpoint());
                    // conn.reply(new Reply());
                }

            });
            Threading.sleep(5000);
        }

    }
}
