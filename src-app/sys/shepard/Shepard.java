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
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.logging.Logger;

import sys.net.api.Endpoint;
import sys.net.api.Networking.TransportProvider;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandle;
import sys.scheduler.Task;
import sys.shepard.proto.GrazingAccepted;
import sys.shepard.proto.GrazingGranted;
import sys.shepard.proto.GrazingRequest;
import sys.shepard.proto.ShepardProtoHandler;
import sys.utils.Args;
import sys.utils.IP;
import sys.utils.Threading;

/**
 * 
 * Simple manager for coordinating execution of multiple swiftsocial instances
 * in a single deployment...
 * 
 * @author smduarte
 * 
 */
public class Shepard extends ShepardProtoHandler {
    private static Logger Log = Logger.getLogger(Shepard.class.getName());

    private static final int PORT = 29876;

    int totalSheep;
    int grazingDuration;

    RpcEndpoint endpoint;
    List<RpcHandle> waitingSheep;

    public Shepard() {
    }

    public Shepard(int duration) {
        this.grazingDuration = duration;
        this.waitingSheep = new ArrayList<RpcHandle>();
        this.endpoint = Networking.rpcBind(PORT, TransportProvider.DEFAULT).toService(0, this);
    }

    public static void main(String[] args) {
        sys.Sys.init();
        int duration = Args.valueOf(args, "-duration", Integer.MAX_VALUE);

        new Shepard(duration);
    }

    public void joinHerd(String shepardAddress) {
        Endpoint shepard = Networking.resolve(shepardAddress, PORT);
        RpcEndpoint endpoint = Networking.rpcConnect(TransportProvider.DEFAULT).toDefaultService();
        System.err.println("Contacting shepard at: " + shepardAddress);

        final Semaphore barrier = new Semaphore(0);
        endpoint.send(shepard, new GrazingRequest(), new ShepardProtoHandler() {

            public void onReceive(GrazingAccepted r) {
                System.err.println("Got ack from shepard...");
            }

            public void onReceive(GrazingGranted permission) {
                System.err.println("Got from shepard; when:" + permission.when() + "/ duration:"
                        + permission.duration());
                Threading.sleep(permission.when() + new java.util.Random().nextInt(5000));
                barrier.release();
                new Task(permission.duration()) {
                    public void run() {
                        Log.info(IP.localHostAddressString() + " Meh...I'm done...");
                        System.exit(0);
                    }
                };
            }
        }, 0).enableDeferredReplies(1200000);
        barrier.acquireUninterruptibly();
        Log.info(IP.localHostAddressString() + " Let's GO!!!!!");
        System.err.println(IP.localHostAddressString() + " Let's GO!!!!!");
    }

    Task releaseTask = new Task(Double.MAX_VALUE) {
        public void run() {
            synchronized (Shepard.this) {
                System.err.printf("SHEPARD: Releasing: %d sheep\n", waitingSheep.size());
                if (releaseTime < 0) {
                    releaseTime = System.currentTimeMillis();
                }
                for (RpcHandle i : waitingSheep) {
                    long when = Math.max(0, 15000 - (System.currentTimeMillis() - releaseTime));
                    i.reply(new GrazingGranted(grazingDuration, (int) when));
                }

                System.err.printf("SHEPARD: Released: %d sheep\n", waitingSheep.size());
                waitingSheep.clear();
            }
        }
    };
    long releaseTime = -1;

    synchronized public void onReceive(RpcHandle sheep, GrazingRequest request) {
        sheep.enableDeferredReplies(Integer.MAX_VALUE);
        sheep.reply(new GrazingAccepted());
        waitingSheep.add(sheep);
        releaseTask.reSchedule(releaseTime < 0 ? 20.0 : 0);
        System.err.printf("SHEPARD: Added one more sheep: %s, total: %d sheep\n", sheep.remoteEndpoint(),
                waitingSheep.size());

    }
}
