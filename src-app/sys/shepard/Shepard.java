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
package sys.shepard;

import static sys.net.api.Networking.Networking;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.logging.Logger;

import swift.application.filesystem.cs.SwiftFuseServer;
import swift.client.SwiftImpl;
import sys.net.api.Endpoint;
import sys.net.api.Networking.TransportProvider;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandle;
import sys.scheduler.Task;
import sys.shepard.proto.GrazingGranted;
import sys.shepard.proto.GrazingRequest;
import sys.shepard.proto.GrazingAccepted;
import sys.shepard.proto.ShepardProtoHandler;
import sys.utils.Args;
import sys.utils.IP;
import sys.utils.Threading;

public class Shepard extends ShepardProtoHandler {
    private static Logger Log = Logger.getLogger(Shepard.class.getName());

    private static final int PORT = 9876;

    int totalSheep;
    int grazingDuration;

    RpcEndpoint endpoint;
    List<RpcHandle> waitingSheep;

    public Shepard() {
    }

    public Shepard(int sheep, int duration) {
        this.totalSheep = sheep;
        this.grazingDuration = duration;
        this.waitingSheep = new ArrayList<RpcHandle>();
        this.endpoint = Networking.rpcBind(PORT, TransportProvider.DEFAULT).toService(0, this);
    }

    public static void main(String[] args) {
        sys.Sys.init();
        int sheep = Args.valueOf(args, "-sheep", 1);
        int duration = Args.valueOf(args, "-duration", Integer.MAX_VALUE);
        new Shepard(sheep, duration);
    }

    public void joinHerd(String shepardAddress) {
        Endpoint shepard = Networking.resolve(shepardAddress, PORT);
        RpcEndpoint endpoint = Networking.rpcConnect(TransportProvider.DEFAULT).toDefaultService();

        final Semaphore barrier = new Semaphore(0);
        endpoint.send(shepard, new GrazingRequest(), new ShepardProtoHandler() {
            public void onReceive(GrazingGranted permission) {
                barrier.release();
                new Task(permission.duration()) {
                    public void run() {
                        Log.info(IP.localHostAddressString() + " Meh...I'm done...");
                        System.exit(0);
                    }
                };
            }
        });
        barrier.acquireUninterruptibly();
        Log.info( IP.localHostAddressString() + " Let's GO!!!!!");        
    }

    synchronized public void onReceive(RpcHandle sheep, GrazingRequest request) {
        sheep.enableDeferredReplies(Integer.MAX_VALUE);
        sheep.reply(new GrazingAccepted());

        waitingSheep.add(sheep);

        if (waitingSheep.size() == totalSheep) {// Release the sheep
            Log.info("Releasing the sheep...");
            for (RpcHandle i : waitingSheep)
                i.reply(new GrazingGranted(grazingDuration));

            waitingSheep.clear();
            // Get ready for next herd...
            Log.info("Waiting for next herd...");
        }
    }
}
