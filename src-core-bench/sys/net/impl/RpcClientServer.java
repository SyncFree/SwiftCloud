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
package sys.net.impl;

import java.net.UnknownHostException;
import java.util.logging.Logger;

import sys.utils.Threading;

public class RpcClientServer {
    public static Logger Log = Logger.getLogger(RpcClientServer.class.getName());

    public static void main(final String[] args) throws UnknownHostException {

        KryoLib.register(Request.class, 0x100);
        KryoLib.register(Reply.class, 0x101);

        RpcServer.main(new String[0]);

        for (int i = 0; i < 1; i++)
            Threading.newThread(true, new Runnable() {
                public void run() {
                    try {
                        RpcClient.main(new String[0]);
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }
            }).start();

    }
}
